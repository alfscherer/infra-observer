package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/dop251/goja"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// requestedEvent is what a script may ask to emit. Emission is deferred: the
// host validates and records the request, and the Go caller decides what to do
// with the resulting events after the script returns.
type requestedEvent struct {
	Type     string            `json:"type"`
	Severity string            `json:"severity"`
	Message  string            `json:"message"`
	DeviceID string            `json:"device_id"`
	Labels   map[string]string `json:"labels"`
}

func validLabels(l map[string]string) error {
	if len(l) > MaxLogFields {
		return fmt.Errorf("at most %d labels", MaxLogFields)
	}
	for k, v := range l {
		if k == "" || len(k) > 64 || len(v) > MaxLogFieldValue {
			return fmt.Errorf("label %q is too long or empty", k)
		}
	}
	return nil
}

func (h *host) knownDevice(id string) bool {
	if h.d.Inventory == nil {
		return true
	}
	_, ok := h.d.Inventory.Get(id)
	return ok
}

func (h *host) installEvents() {
	h.object("events", map[string]func(goja.FunctionCall) goja.Value{
		"emit": func(fc goja.FunctionCall) goja.Value {
			c := h.require(permissions[h.call().Script.Kind].Events, "events.emit")
			var r requestedEvent
			if err := h.fromJS(fc.Argument(0), &r); err != nil {
				h.throw("events.emit: %v", err)
			}
			if len(c.events) >= MaxEvents {
				h.throw("events.emit: at most %d events per invocation", MaxEvents)
			}
			if r.DeviceID == "" {
				r.DeviceID = c.DeviceID
			}
			sev := domain.Severity(r.Severity)
			switch {
			case !eventTypeRe.MatchString(r.Type):
				h.throw("events.emit: type must be dotted lower-case, e.g. ticket.created")
			case !sev.Valid():
				h.throw("events.emit: severity must be info, warning or critical")
			case r.DeviceID == "" || !h.knownDevice(r.DeviceID):
				h.throw("events.emit: unknown device %q", r.DeviceID)
			case len(r.Message) > MaxLogMessage:
				h.throw("events.emit: message too long")
			}
			if err := validLabels(r.Labels); err != nil {
				h.throw("events.emit: %v", err)
			}
			c.events = append(c.events, domain.Event{
				EventID:       domain.StableID("evt", "script", c.Script.Key, r.DeviceID, r.Type, domain.LabelsKey(r.Labels), c.Trigger),
				CorrelationID: c.CorrelationID, DeviceID: r.DeviceID, Type: r.Type, Severity: sev, Message: r.Message,
				Labels: r.Labels, OccurredAt: h.d.now().UTC(), ObservationID: "", Source: "script:" + c.Script.ID,
			})
			return goja.Undefined()
		},
	})
}

type requestedMetric struct {
	Metric   string            `json:"metric"`
	Value    any               `json:"value"`
	Labels   map[string]string `json:"labels"`
	DeviceID string            `json:"device_id"`
}

func (h *host) installMetrics() {
	h.object("metrics", map[string]func(goja.FunctionCall) goja.Value{
		"emit": func(fc goja.FunctionCall) goja.Value {
			k := h.call().Script.Kind
			c := h.require(permissions[k].Metrics, "metrics.emit")
			var r requestedMetric
			if err := h.fromJS(fc.Argument(0), &r); err != nil {
				h.throw("metrics.emit: %v", err)
			}
			if len(c.observations) >= MaxObservations {
				h.throw("metrics.emit: at most %d observations per invocation", MaxObservations)
			}
			if r.DeviceID == "" {
				r.DeviceID = c.DeviceID
			}
			if !h.knownDevice(r.DeviceID) {
				h.throw("metrics.emit: unknown device %q", r.DeviceID)
			}
			corr := c.CorrelationID
			if corr == "" {
				corr = domain.StableID("corr", c.Script.Key, c.Trigger)
			}
			o := domain.Observation{
				ObservationID: domain.NewObservationID(domain.StableID(c.Script.Key, c.Trigger), r.DeviceID, r.Metric, r.Labels),
				CorrelationID: corr, DeviceID: r.DeviceID, Source: "script:" + c.Script.ID, Metric: r.Metric,
				Value: r.Value, Labels: r.Labels, ObservedAt: h.d.now().UTC(),
			}
			if err := h.d.Validator.Validate(o); err != nil {
				h.throw("metrics.emit: %v", err)
			}
			c.observations = append(c.observations, o)
			return goja.Undefined()
		},
	})
}

type requestedHTTP struct {
	Endpoint  string            `json:"endpoint"`
	Method    string            `json:"method"`
	Path      string            `json:"path"`
	Headers   map[string]string `json:"headers"`
	Body      json.RawMessage   `json:"body"`
	TimeoutMS int               `json:"timeout_ms"`
}

var allowedHeaders = map[string]string{"content-type": "Content-Type", "accept": "Accept", "x-request-id": "X-Request-Id"}

// AllowedEndpoints returns the endpoint names a script's configuration
// authorises: any config key ending in "endpoint_ref" (a string) or
// "endpoint_refs" (a list of strings). Authorisation lives in configuration,
// beside the script, not in the script's source.
func AllowedEndpoints(cfg map[string]any) []string {
	var out []string
	for k, v := range cfg {
		switch {
		case strings.HasSuffix(k, "endpoint_ref"):
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		case strings.HasSuffix(k, "endpoint_refs"):
			if l, ok := v.([]any); ok {
				for _, e := range l {
					if s, ok := e.(string); ok {
						out = append(out, s)
					}
				}
			}
		}
	}
	return out
}

func (h *host) installHTTP() {
	h.object("http", map[string]func(goja.FunctionCall) goja.Value{
		"request": func(fc goja.FunctionCall) goja.Value {
			c := h.require(permissions[h.call().Script.Kind].HTTP, "http.request")
			var r requestedHTTP
			if err := h.fromJS(fc.Argument(0), &r); err != nil {
				h.throw("http.request: %v", err)
			}
			if c.httpCalls >= MaxHTTPCalls {
				h.throw("http.request: at most %d requests per invocation", MaxHTTPCalls)
			}
			c.httpCalls++

			allowed := false
			for _, e := range AllowedEndpoints(c.Script.Config) {
				if e == r.Endpoint {
					allowed = true
				}
			}
			ep, configured := h.d.Endpoints[r.Endpoint]
			if !allowed || !configured || h.d.Secrets == nil {
				// One message for "not authorised" and "does not exist": a script
				// learns nothing about endpoints it has not been granted.
				h.throw("http.request: endpoint %q is not available to this script", r.Endpoint)
			}
			method := strings.ToUpper(r.Method)
			if method == "" {
				method = http.MethodGet
			}
			if method != http.MethodGet && method != http.MethodPost && method != http.MethodPut {
				h.throw("http.request: method %q is not allowed (GET, POST, PUT)", r.Method)
			}
			if !strings.HasPrefix(r.Path, "/") || strings.HasPrefix(r.Path, "//") || strings.Contains(r.Path, "://") || strings.ContainsAny(r.Path, "\r\n ") {
				h.throw("http.request: path must be an absolute path such as /v1/events")
			}
			hdrs := http.Header{}
			for k, v := range r.Headers {
				canon, ok := allowedHeaders[strings.ToLower(k)]
				if !ok {
					h.throw("http.request: header %q is not allowed", k)
				}
				hdrs.Set(canon, v)
			}
			var body io.Reader
			if len(r.Body) > 0 && string(r.Body) != "null" {
				var payload []byte
				var s string
				if json.Unmarshal(r.Body, &s) == nil {
					payload = []byte(s)
				} else {
					payload = r.Body
					if hdrs.Get("Content-Type") == "" {
						hdrs.Set("Content-Type", "application/json")
					}
				}
				if len(payload) > MaxRequestBody {
					h.throw("http.request: body exceeds %d bytes", MaxRequestBody)
				}
				body = bytes.NewReader(payload)
			}

			secret, err := h.d.Secrets.Resolve(h.ctx(), ep.URLRef)
			if err != nil {
				h.throw("http.request: endpoint %q is not available to this script", r.Endpoint)
			}
			base := secret.Get("url")
			if base == "" {
				base = secret.Get("value")
			}
			bu, err := url.Parse(base)
			if err != nil || (bu.Scheme != "http" && bu.Scheme != "https") || bu.Host == "" {
				h.throw("http.request: endpoint %q is misconfigured", r.Endpoint)
			}
			target := *bu
			target.Path = strings.TrimRight(bu.Path, "/") + r.Path
			if q := strings.SplitN(r.Path, "?", 2); len(q) == 2 {
				target.Path, target.RawQuery = strings.TrimRight(bu.Path, "/")+q[0], q[1]
			}
			if target.Host != bu.Host {
				h.throw("http.request: request would leave the endpoint")
			}

			timeout := ep.Timeout
			if timeout <= 0 {
				timeout = defaultHTTPTimeout
			}
			if r.TimeoutMS > 0 && time.Duration(r.TimeoutMS)*time.Millisecond < timeout {
				timeout = time.Duration(r.TimeoutMS) * time.Millisecond
			}
			ctx, cancel := contextWithTimeout(h.ctx(), timeout)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, method, target.String(), body)
			if err != nil {
				h.throw("http.request: cannot build request")
			}
			req.Header = hdrs
			// The credential is attached here, by Go, after the script's request
			// has been validated. The script never sees it.
			if tok := secret.Get("token"); tok != "" {
				req.Header.Set("Authorization", "Bearer "+tok)
			}
			req.Header.Set("User-Agent", "infra-observer-script/1")
			// Delivery to an integration is at-least-once. A stable key per
			// (script, trigger) lets the receiver drop the duplicate.
			if c.Trigger != "" {
				req.Header.Set("Idempotency-Key", domain.StableID("idem", c.Script.Key, c.Trigger))
			}
			if c.CorrelationID != "" && req.Header.Get("X-Request-Id") == "" {
				req.Header.Set("X-Request-Id", c.CorrelationID)
			}

			start := time.Now()
			resp, err := h.d.HTTP.Do(req)
			if err != nil {
				if h.d.OnHTTP != nil {
					h.d.OnHTTP(c.Script.Key, r.Endpoint, 0, time.Since(start), err)
				}
				// The error text can contain the (secret) URL: do not pass it on.
				h.throw("http.request: request to endpoint %q failed (%s)", r.Endpoint, failureKind(ctx, err))
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBody+1))
			truncated := len(data) > MaxResponseBody
			if truncated {
				data = data[:MaxResponseBody]
			}
			if h.d.OnHTTP != nil {
				h.d.OnHTTP(c.Script.Key, r.Endpoint, resp.StatusCode, time.Since(start), nil)
			}
			return h.toJS(map[string]any{
				"status": resp.StatusCode, "ok": resp.StatusCode >= 200 && resp.StatusCode < 300,
				"body": string(data), "truncated": truncated, "content_type": resp.Header.Get("Content-Type"),
			})
		},
	})
}

func failureKind(ctx interface{ Err() error }, err error) string {
	if ctx.Err() != nil {
		return "timeout"
	}
	return "network error"
}
