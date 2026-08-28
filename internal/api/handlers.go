package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/persistence"
)

func (s *Server) pageParams(r *http.Request) (persistence.Page, error) {
	def, max := s.DefaultPageSize, s.MaxPageSize
	if def <= 0 {
		def = 50
	}
	if max <= 0 {
		max = 500
	}
	limit := def
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > max {
			return persistence.Page{}, domain.Errorf(domain.CategoryValidation, "limit must be an integer between 1 and %d", max)
		}
		limit = n
	}
	return persistence.Page{Limit: limit, Cursor: r.URL.Query().Get("cursor")}, nil
}

func timeParam(r *http.Request, name string) (time.Time, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, domain.Errorf(domain.CategoryValidation, "%s must be an RFC 3339 timestamp, e.g. 2026-08-01T12:00:00Z", name)
	}
	return t, nil
}

func enumParam(r *http.Request, name string, allowed ...string) (string, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return "", nil
	}
	for _, a := range allowed {
		if v == a {
			return v, nil
		}
	}
	return "", domain.Errorf(domain.CategoryValidation, "%s must be one of %v", name, allowed)
}

// --- devices ---------------------------------------------------------------------

func (s *Server) listDevices(w http.ResponseWriter, r *http.Request) {
	devices, err := s.Store.ListDevices(r.Context())
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	q := r.URL.Query()
	var filtered []DeviceDTO
	for _, d := range devices {
		if (q.Get("site") != "" && d.Site != q.Get("site")) || (q.Get("type") != "" && string(d.DeviceType) != q.Get("type")) ||
			(q.Get("tag") != "" && !d.HasTag(q.Get("tag"))) || (q.Get("enabled") != "" && strconv.FormatBool(d.Enabled) != q.Get("enabled")) {
			continue
		}
		filtered = append(filtered, deviceDTO(d))
	}
	// Devices are few (hundreds, not millions) and sorted by id, so a cursor is
	// simply "the last id seen".
	p, err := s.pageParams(r)
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	start := 0
	if p.Cursor != "" {
		start = len(filtered)
		for i, d := range filtered {
			if d.ID > p.Cursor {
				start = i
				break
			}
		}
	}
	end := min(start+p.Limit, len(filtered))
	next := ""
	if end < len(filtered) {
		next = filtered[end-1].ID
	}
	writeJSON(w, http.StatusOK, page(filtered[start:end], next))
}

func (s *Server) findDevice(w http.ResponseWriter, r *http.Request) (domain.Device, bool) {
	id := r.PathValue("id")
	devices, err := s.Store.ListDevices(r.Context())
	if err != nil {
		s.failErr(w, r, err)
		return domain.Device{}, false
	}
	for _, d := range devices {
		if d.ID == id {
			return d, true
		}
	}
	s.fail(w, r, http.StatusNotFound, "not_found", "no such device")
	return domain.Device{}, false
}

func (s *Server) getDevice(w http.ResponseWriter, r *http.Request) {
	d, ok := s.findDevice(w, r)
	if !ok {
		return
	}
	dto := deviceDTO(d)
	firing, _, err := s.Store.ListAlerts(r.Context(), persistence.AlertFilter{Status: domain.AlertFiring, DeviceID: d.ID}, persistence.Page{Limit: 1000})
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	n := len(firing)
	dto.OpenAlerts = &n
	writeJSON(w, http.StatusOK, dto)
}

// getDeviceState summarises derived state. The overall health is deliberately
// coarse: down if any tracked entity is DOWN, degraded if any is not UP.
func (s *Server) getDeviceState(w http.ResponseWriter, r *http.Request) {
	d, ok := s.findDevice(w, r)
	if !ok {
		return
	}
	recs, err := s.Store.DeviceStates(r.Context(), d.ID)
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	out := DeviceStateDTO{DeviceID: d.ID, Health: "healthy", States: []StateDTO{}}
	for _, rec := range recs {
		out.States = append(out.States, stateDTO(rec))
		switch rec.State {
		case domain.StateDown:
			out.Health = "down"
		case domain.StateUp:
		default:
			if out.Health == "healthy" {
				out.Health = "degraded"
			}
		}
	}
	if len(recs) == 0 {
		out.Health = "unknown"
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getDeviceMetrics(w http.ResponseWriter, r *http.Request) {
	d, ok := s.findDevice(w, r)
	if !ok {
		return
	}
	since, err := timeParam(r, "since")
	if err == nil {
		var until time.Time
		if until, err = timeParam(r, "until"); err == nil {
			var p persistence.Page
			if p, err = s.pageParams(r); err == nil {
				var points []persistence.MetricPoint
				if metric := r.URL.Query().Get("metric"); metric != "" {
					points, err = s.Store.Metrics(r.Context(), d.ID, metric, since, until, p.Limit)
				} else {
					points, err = s.Store.LatestMetrics(r.Context(), d.ID)
				}
				if err == nil {
					out := make([]MetricDTO, len(points))
					for i, pt := range points {
						out[i] = metricDTO(pt)
					}
					writeJSON(w, http.StatusOK, map[string]any{"device_id": d.ID, "metric": r.URL.Query().Get("metric"), "points": out})
					return
				}
			}
		}
	}
	s.failErr(w, r, err)
}

// --- events, alerts -----------------------------------------------------------------

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	p, err := s.pageParams(r)
	var since, until time.Time
	var sev string
	if err == nil {
		if since, err = timeParam(r, "since"); err == nil {
			if until, err = timeParam(r, "until"); err == nil {
				sev, err = enumParam(r, "min_severity", "info", "warning", "critical")
			}
		}
	}
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	items, next, err := s.Store.ListEvents(r.Context(), persistence.EventFilter{
		DeviceID: r.URL.Query().Get("device"), Type: r.URL.Query().Get("type"), MinSeverity: domain.Severity(sev), Since: since, Until: until,
	}, p)
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	out := make([]EventDTO, len(items))
	for i, e := range items {
		out[i] = eventDTO(e)
	}
	writeJSON(w, http.StatusOK, page(out, next))
}

func (s *Server) listAlerts(w http.ResponseWriter, r *http.Request) {
	p, err := s.pageParams(r)
	var status string
	if err == nil {
		status, err = enumParam(r, "status", "firing", "resolved")
	}
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	items, next, err := s.Store.ListAlerts(r.Context(), persistence.AlertFilter{Status: domain.AlertStatus(status), DeviceID: r.URL.Query().Get("device")}, p)
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	out := make([]AlertDTO, len(items))
	for i, a := range items {
		out[i] = alertDTO(a)
	}
	writeJSON(w, http.StatusOK, page(out, next))
}

// --- automation ---------------------------------------------------------------------

var automationStatuses = []string{"pending", "awaiting_approval", "approved", "executing", "succeeded", "failed", "dry_run", "denied", "expired"}

func (s *Server) listAutomation(w http.ResponseWriter, r *http.Request) {
	p, err := s.pageParams(r)
	var status string
	if err == nil {
		status, err = enumParam(r, "status", automationStatuses...)
	}
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	items, next, err := s.Store.ListAutomation(r.Context(), persistence.AutomationFilter{
		Status: domain.AutomationStatus(status), DeviceID: r.URL.Query().Get("device"), PolicyID: r.URL.Query().Get("policy"),
	}, p)
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	out := make([]AutomationDTO, len(items))
	for i, v := range items {
		out[i] = automationDTO(v)
	}
	writeJSON(w, http.StatusOK, page(out, next))
}

func (s *Server) getAutomation(w http.ResponseWriter, r *http.Request) {
	req, res, err := s.Store.GetAutomation(r.Context(), r.PathValue("id"))
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	if req == nil {
		s.fail(w, r, http.StatusNotFound, "not_found", "no such automation request")
		return
	}
	writeJSON(w, http.StatusOK, automationFrom(*req, res))
}

// approve is the API's only write. The approver's identity comes from the
// bearer token, never from the request body, so it cannot be claimed by
// whoever is calling.
func (s *Server) approve(w http.ResponseWriter, r *http.Request) {
	if s.Auth == nil || s.Approver == nil {
		s.fail(w, r, http.StatusForbidden, "approvals_disabled", "approvals are not enabled on this deployment")
		return
	}
	who, ok := s.Auth.Identify(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", `Bearer realm="infra-observer"`)
		s.fail(w, r, http.StatusUnauthorized, "unauthorized", "a valid bearer token is required")
		return
	}
	id := r.PathValue("id")
	req, _, err := s.Store.GetAutomation(r.Context(), id)
	if err != nil {
		s.failErr(w, r, err)
		return
	}
	if req == nil {
		s.fail(w, r, http.StatusNotFound, "not_found", "no such automation request")
		return
	}
	if err := s.Approver.Approve(r.Context(), id, who); err != nil {
		if domain.CategoryOf(err) == domain.CategoryValidation {
			s.fail(w, r, http.StatusConflict, "not_awaiting_approval", "the request is not awaiting approval (already approved, expired or completed)")
			return
		}
		s.failErr(w, r, err)
		return
	}
	s.log().Info("automation approved via API", "automation_id", id, "approved_by", who, "request_id", w.Header().Get("X-Request-Id"))
	updated, res, err := s.Store.GetAutomation(r.Context(), id)
	if err != nil || updated == nil {
		s.failErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, automationFrom(*updated, res))
}
