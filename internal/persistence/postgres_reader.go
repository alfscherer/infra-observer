package persistence

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// where accumulates SQL conditions with numbered arguments.
type where struct {
	conds []string
	args  []any
}

func (w *where) add(cond string, v any) {
	w.args = append(w.args, v)
	w.conds = append(w.conds, strings.ReplaceAll(cond, "?", fmt.Sprintf("$%d", len(w.args))))
}

func (w *where) sql() string {
	if len(w.conds) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(w.conds, " AND ")
}

func (w *where) arg(v any) string { w.args = append(w.args, v); return fmt.Sprintf("$%d", len(w.args)) }

func (s *PGStore) DeviceStates(ctx context.Context, deviceID string) ([]domain.StateRecord, error) {
	all, err := s.ListStates(ctx)
	if err != nil {
		return nil, err
	}
	var out []domain.StateRecord
	for _, r := range all {
		if r.DeviceID == deviceID {
			out = append(out, r)
		}
	}
	return out, nil
}

func (s *PGStore) ListEvents(ctx context.Context, f EventFilter, p Page) ([]domain.Event, string, error) {
	ct, cid, err := DecodeCursor(p.Cursor)
	if err != nil {
		return nil, "", err
	}
	limit := clampLimit(p.Limit)
	var w where
	if f.DeviceID != "" {
		w.add("device_id = ?", f.DeviceID)
	}
	if f.Type != "" {
		w.add("type = ?", f.Type)
	}
	if f.MinSeverity != "" {
		w.add("severity = ANY(?)", severitiesFrom(f.MinSeverity))
	}
	if !f.Since.IsZero() {
		w.add("occurred_at >= ?", f.Since)
	}
	if !f.Until.IsZero() {
		w.add("occurred_at < ?", f.Until)
	}
	if p.Cursor != "" {
		t, id := w.arg(ct), w.arg(cid)
		w.conds = append(w.conds, fmt.Sprintf("(occurred_at, event_id) < (%s, %s)", t, id))
	}
	rows, err := s.pool.Query(ctx, `SELECT event_id, correlation_id, device_id, type, severity, message, labels, occurred_at, observation_id, source, alert_key, resolves
		FROM events`+w.sql()+fmt.Sprintf(` ORDER BY occurred_at DESC, event_id DESC LIMIT %d`, limit+1), w.args...)
	if err != nil {
		return nil, "", s.fail("list events", err)
	}
	defer rows.Close()
	var out []domain.Event
	for rows.Next() {
		var e domain.Event
		var sev string
		var labels []byte
		if err := rows.Scan(&e.EventID, &e.CorrelationID, &e.DeviceID, &e.Type, &sev, &e.Message, &labels, &e.OccurredAt, &e.ObservationID, &e.Source, &e.AlertKey, &e.Resolves); err != nil {
			return nil, "", s.fail("scan event", err)
		}
		e.Severity, e.Labels = domain.Severity(sev), decodeLabels(labels)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", s.fail("list events", err)
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		next = EncodeCursor(last.OccurredAt, last.EventID)
	}
	return out, next, nil
}

func (s *PGStore) ListAlerts(ctx context.Context, f AlertFilter, p Page) ([]domain.Alert, string, error) {
	ct, cid, err := DecodeCursor(p.Cursor)
	if err != nil {
		return nil, "", err
	}
	limit := clampLimit(p.Limit)
	var w where
	if f.Status != "" {
		w.add("status = ?", string(f.Status))
	}
	if f.DeviceID != "" {
		w.add("device_id = ?", f.DeviceID)
	}
	if p.Cursor != "" {
		t, id := w.arg(ct), w.arg(cid)
		w.conds = append(w.conds, fmt.Sprintf("(opened_at, open_event_id) < (%s, %s)", t, id))
	}
	rows, err := s.pool.Query(ctx, `SELECT open_event_id, alert_key, device_id, source, severity, status, summary, labels, opened_at, resolved_at
		FROM alerts`+w.sql()+fmt.Sprintf(` ORDER BY opened_at DESC, open_event_id DESC LIMIT %d`, limit+1), w.args...)
	if err != nil {
		return nil, "", s.fail("list alerts", err)
	}
	defer rows.Close()
	var out []domain.Alert
	for rows.Next() {
		var a domain.Alert
		var sev, status string
		var labels []byte
		var resolved *time.Time
		if err := rows.Scan(&a.OpenEventID, &a.AlertKey, &a.DeviceID, &a.Source, &sev, &status, &a.Summary, &labels, &a.OpenedAt, &resolved); err != nil {
			return nil, "", s.fail("scan alert", err)
		}
		a.Severity, a.Status, a.Labels = domain.Severity(sev), domain.AlertStatus(status), decodeLabels(labels)
		if resolved != nil {
			a.ResolvedAt = *resolved
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, "", s.fail("list alerts", err)
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		next = EncodeCursor(last.OpenedAt, last.OpenEventID)
	}
	return out, next, nil
}

func (s *PGStore) ListAutomation(ctx context.Context, f AutomationFilter, p Page) ([]AutomationView, string, error) {
	ct, cid, err := DecodeCursor(p.Cursor)
	if err != nil {
		return nil, "", err
	}
	limit := clampLimit(p.Limit)
	var w where
	if f.Status != "" {
		w.add("r.status = ?", string(f.Status))
	}
	if f.DeviceID != "" {
		w.add("r.device_id = ?", f.DeviceID)
	}
	if f.PolicyID != "" {
		w.add("r.policy_id = ?", f.PolicyID)
	}
	if p.Cursor != "" {
		t, id := w.arg(ct), w.arg(cid)
		w.conds = append(w.conds, fmt.Sprintf("(r.created_at, r.request_id) < (%s, %s)", t, id))
	}
	rows, err := s.pool.Query(ctx, `SELECT `+prefixCols("r.")+`,
			x.correlation_id, x.status, x.dry_run, x.message, x.details, x.started_at, x.finished_at
		FROM automation_requests r LEFT JOIN automation_results x USING (request_id)`+w.sql()+
		fmt.Sprintf(` ORDER BY r.created_at DESC, r.request_id DESC LIMIT %d`, limit+1), w.args...)
	if err != nil {
		return nil, "", s.fail("list automation", err)
	}
	defer rows.Close()
	var out []AutomationView
	for rows.Next() {
		var (
			r       domain.AutomationRequest
			params  []byte
			status  string
			xcorr   *string
			xstatus *string
			xdry    *bool
			xmsg    *string
			xdet    []byte
			xstart  *time.Time
			xend    *time.Time
		)
		if err := rows.Scan(&r.RequestID, &r.CorrelationID, &r.PolicyID, &r.EventID, &r.AlertKey, &r.DeviceID, &r.Action, &r.Target, &params, &r.Reason,
			&r.Proposer, &r.DryRun, &status, &r.NotBefore, &r.CreatedAt, &r.ApprovedBy, &xcorr, &xstatus, &xdry, &xmsg, &xdet, &xstart, &xend); err != nil {
			return nil, "", s.fail("scan automation", err)
		}
		r.Status, r.Params = domain.AutomationStatus(status), decodeLabels(params)
		v := AutomationView{Request: r}
		if xstatus != nil {
			v.Result = &domain.AutomationResult{RequestID: r.RequestID, CorrelationID: *xcorr, Status: domain.AutomationStatus(*xstatus), DryRun: *xdry,
				Message: *xmsg, Details: decodeLabels(xdet), StartedAt: *xstart, FinishedAt: *xend}
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, "", s.fail("list automation", err)
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1].Request
		next = EncodeCursor(last.CreatedAt, last.RequestID)
	}
	return out, next, nil
}

func scanPoint(rows pgx.Rows) (MetricPoint, error) {
	var (
		p   MetricPoint
		num *float64
		bl  *bool
		txt *string
		lbl []byte
	)
	if err := rows.Scan(&p.ObservationID, &p.Metric, &num, &bl, &txt, &lbl, &p.ObservedAt); err != nil {
		return p, err
	}
	switch {
	case bl != nil:
		p.Value = *bl
	case txt != nil:
		p.Value = *txt
	case num != nil:
		p.Value = *num
	}
	p.Labels = decodeLabels(lbl)
	return p, nil
}

func (s *PGStore) Metrics(ctx context.Context, deviceID, metric string, since, until time.Time, limit int) ([]MetricPoint, error) {
	w := where{}
	w.add("device_id = ?", deviceID)
	w.add("metric = ?", metric)
	if !since.IsZero() {
		w.add("observed_at >= ?", since)
	}
	if !until.IsZero() {
		w.add("observed_at < ?", until)
	}
	rows, err := s.pool.Query(ctx, `SELECT observation_id, metric, value_num, value_bool, value_text, labels, observed_at FROM observations`+
		w.sql()+fmt.Sprintf(` ORDER BY observed_at DESC LIMIT %d`, clampLimit(limit)), w.args...)
	if err != nil {
		return nil, s.fail("metrics", err)
	}
	defer rows.Close()
	var out []MetricPoint
	for rows.Next() {
		p, err := scanPoint(rows)
		if err != nil {
			return nil, s.fail("scan metric", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *PGStore) LatestMetrics(ctx context.Context, deviceID string) ([]MetricPoint, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT ON (metric, labels_key) observation_id, metric, value_num, value_bool, value_text, labels, observed_at
		FROM observations WHERE device_id = $1 ORDER BY metric, labels_key, observed_at DESC`, deviceID)
	if err != nil {
		return nil, s.fail("latest metrics", err)
	}
	defer rows.Close()
	var out []MetricPoint
	for rows.Next() {
		p, err := scanPoint(rows)
		if err != nil {
			return nil, s.fail("scan metric", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
