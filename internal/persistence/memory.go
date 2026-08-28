package persistence

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// MemStore is an in-memory Store. Do holds a global lock and rolls back through
// an undo log, giving the same atomicity as a database transaction for tests.
type MemStore struct {
	mu          sync.Mutex
	devices     map[string]domain.Device
	observation map[string]domain.Observation
	obsOrder    []string
	states      map[string]domain.StateRecord
	transitions map[string]domain.Transition
	trOrder     []string
	events      map[string]domain.Event
	evOrder     []string
	alerts      []domain.Alert
	outbox      []outboxRow
	nextOutbox  int64
	autoReq     map[string]*domain.AutomationRequest
	autoOrder   []string
	autoRes     map[string]domain.AutomationResult
	autoClaim   map[string]time.Time

	// FailNext, when set, makes the next Do return this error before running fn
	// (used to simulate a database outage).
	failNext error
}

type outboxRow struct {
	msg       OutboxMessage
	published bool
}

// NewMemStore returns an empty store.
func NewMemStore() *MemStore {
	return &MemStore{
		devices: map[string]domain.Device{}, observation: map[string]domain.Observation{},
		states: map[string]domain.StateRecord{}, transitions: map[string]domain.Transition{},
		events:  map[string]domain.Event{},
		autoReq: map[string]*domain.AutomationRequest{}, autoRes: map[string]domain.AutomationResult{}, autoClaim: map[string]time.Time{},
	}
}

// FailNext injects an error returned by the next Do call.
func (s *MemStore) FailNext(err error) {
	s.mu.Lock()
	s.failNext = err
	s.mu.Unlock()
}

type memTx struct {
	s    *MemStore
	undo []func()
}

func (s *MemStore) Do(ctx context.Context, fn func(Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failNext != nil {
		err := s.failNext
		s.failNext = nil
		return err
	}
	if err := ctx.Err(); err != nil {
		return domain.Wrap(domain.CategoryTimeout, "memstore", err)
	}
	tx := &memTx{s: s}
	if err := fn(tx); err != nil {
		for i := len(tx.undo) - 1; i >= 0; i-- {
			tx.undo[i]()
		}
		return err
	}
	return nil
}

func (t *memTx) InsertObservation(_ context.Context, o domain.Observation) (bool, error) {
	s := t.s
	if _, ok := s.observation[o.ObservationID]; ok {
		return false, nil
	}
	s.observation[o.ObservationID] = o.Clone()
	s.obsOrder = append(s.obsOrder, o.ObservationID)
	t.undo = append(t.undo, func() {
		delete(s.observation, o.ObservationID)
		s.obsOrder = s.obsOrder[:len(s.obsOrder)-1]
	})
	return true, nil
}

func (t *memTx) TouchDevice(_ context.Context, id string, at time.Time) error {
	d, ok := t.s.devices[id]
	if !ok || !at.After(d.LastSeen) {
		return nil
	}
	old := d
	d.LastSeen = at
	t.s.devices[id] = d
	t.undo = append(t.undo, func() { t.s.devices[id] = old })
	return nil
}

func (t *memTx) Samples(_ context.Context, deviceID, metric, labelsKey string, from, to time.Time) ([]domain.Sample, error) {
	var out []domain.Sample
	for _, id := range t.s.obsOrder {
		o := t.s.observation[id]
		if o.DeviceID != deviceID || o.Metric != metric || domain.LabelsKey(o.Labels) != labelsKey {
			continue
		}
		if !o.ObservedAt.After(from) || o.ObservedAt.After(to) {
			continue
		}
		f, ok := o.Float()
		if !ok {
			continue
		}
		smp := domain.Sample{At: o.ObservedAt, Num: f}
		if b, isBool := o.Value.(bool); isBool {
			smp.Bool = &b
		}
		out = append(out, smp)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out, nil
}

func (t *memTx) GetState(_ context.Context, key string) (*domain.StateRecord, error) {
	r, ok := t.s.states[key]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

func (t *memTx) PutState(_ context.Context, r domain.StateRecord) error {
	old, had := t.s.states[r.Key]
	t.s.states[r.Key] = r
	t.undo = append(t.undo, func() {
		if had {
			t.s.states[r.Key] = old
		} else {
			delete(t.s.states, r.Key)
		}
	})
	return nil
}

func (t *memTx) InsertTransition(_ context.Context, tr domain.Transition) error {
	if _, ok := t.s.transitions[tr.TransitionID]; ok {
		return nil
	}
	t.s.transitions[tr.TransitionID] = tr
	t.s.trOrder = append(t.s.trOrder, tr.TransitionID)
	t.undo = append(t.undo, func() {
		delete(t.s.transitions, tr.TransitionID)
		t.s.trOrder = t.s.trOrder[:len(t.s.trOrder)-1]
	})
	return nil
}

func (t *memTx) InsertEvent(_ context.Context, e domain.Event) (bool, error) {
	if _, ok := t.s.events[e.EventID]; ok {
		return false, nil
	}
	t.s.events[e.EventID] = e
	t.s.evOrder = append(t.s.evOrder, e.EventID)
	t.undo = append(t.undo, func() {
		delete(t.s.events, e.EventID)
		t.s.evOrder = t.s.evOrder[:len(t.s.evOrder)-1]
	})
	return true, nil
}

func (t *memTx) OpenAlert(_ context.Context, e domain.Event) (bool, error) {
	for _, a := range t.s.alerts {
		if a.AlertKey == e.AlertKey && a.Status == domain.AlertFiring {
			return false, nil
		}
	}
	t.s.alerts = append(t.s.alerts, domain.Alert{
		AlertKey: e.AlertKey, DeviceID: e.DeviceID, Source: e.Source, Severity: e.Severity, Status: domain.AlertFiring,
		Summary: e.Message, Labels: e.Labels, OpenEventID: e.EventID, OpenedAt: e.OccurredAt,
	})
	t.undo = append(t.undo, func() { t.s.alerts = t.s.alerts[:len(t.s.alerts)-1] })
	return true, nil
}

func (t *memTx) ResolveAlert(_ context.Context, e domain.Event) (bool, error) {
	for i := range t.s.alerts {
		a := &t.s.alerts[i]
		if a.AlertKey == e.AlertKey && a.Status == domain.AlertFiring {
			old := *a
			a.Status, a.ResolvedAt = domain.AlertResolved, e.OccurredAt
			idx := i
			t.undo = append(t.undo, func() { t.s.alerts[idx] = old })
			return true, nil
		}
	}
	return false, nil
}

func (t *memTx) Enqueue(_ context.Context, m OutboxMessage) error {
	t.s.nextOutbox++
	m.ID = t.s.nextOutbox
	t.s.outbox = append(t.s.outbox, outboxRow{msg: m})
	t.undo = append(t.undo, func() { t.s.outbox = t.s.outbox[:len(t.s.outbox)-1]; t.s.nextOutbox-- })
	return nil
}

func (s *MemStore) UpsertDevices(_ context.Context, devices []domain.Device) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range devices {
		if old, ok := s.devices[d.ID]; ok {
			d.LastSeen = old.LastSeen
		}
		s.devices[d.ID] = d
	}
	return nil
}

func (s *MemStore) UpdateDeviceAttributes(_ context.Context, id string, attrs map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.devices[id]
	if !ok {
		return nil
	}
	merged := map[string]string{}
	for k, v := range d.Attributes {
		merged[k] = v
	}
	for k, v := range attrs {
		merged[k] = v
	}
	d.Attributes = merged
	s.devices[id] = d
	return nil
}

func (s *MemStore) ListDevices(_ context.Context) ([]domain.Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Device, 0, len(s.devices))
	for _, d := range s.devices {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *MemStore) Latest(_ context.Context, deviceID, metric string, labels map[string]string) (*domain.Observation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *domain.Observation
	for _, id := range s.obsOrder {
		o := s.observation[id]
		if o.DeviceID != deviceID || o.Metric != metric {
			continue
		}
		if labels != nil && domain.LabelsKey(o.Labels) != domain.LabelsKey(labels) {
			continue
		}
		if best == nil || o.ObservedAt.After(best.ObservedAt) {
			c := o.Clone()
			best = &c
		}
	}
	return best, nil
}

func (s *MemStore) ListStates(_ context.Context) ([]domain.StateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.StateRecord, 0, len(s.states))
	for _, r := range s.states {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *MemStore) DrainOutbox(ctx context.Context, limit int, publish func(context.Context, []OutboxMessage) error) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var idx []int
	var batch []OutboxMessage
	for i, r := range s.outbox {
		if !r.published {
			idx = append(idx, i)
			batch = append(batch, r.msg)
			if len(batch) == limit {
				break
			}
		}
	}
	if len(batch) == 0 {
		return 0, nil
	}
	if err := publish(ctx, batch); err != nil {
		return 0, err
	}
	for _, i := range idx {
		s.outbox[i].published = true
	}
	return len(batch), nil
}

func (s *MemStore) Ping(context.Context) error { return nil }
func (s *MemStore) Close()                     {}

// --- inspection helpers for tests -------------------------------------------

// Events returns stored events in insertion order.
func (s *MemStore) Events() []domain.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Event, 0, len(s.evOrder))
	for _, id := range s.evOrder {
		out = append(out, s.events[id])
	}
	return out
}

// Alerts returns a copy of all alerts.
func (s *MemStore) Alerts() []domain.Alert {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.Alert(nil), s.alerts...)
}

// Transitions returns stored transitions in insertion order.
func (s *MemStore) Transitions() []domain.Transition {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Transition, 0, len(s.trOrder))
	for _, id := range s.trOrder {
		out = append(out, s.transitions[id])
	}
	return out
}

// ObservationCount returns the number of stored observations.
func (s *MemStore) ObservationCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.observation)
}

// OutboxPending returns the number of unpublished outbox messages.
func (s *MemStore) OutboxPending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.outbox {
		if !r.published {
			n++
		}
	}
	return n
}

// Device returns a mirrored device.
func (s *MemStore) Device(id string) (domain.Device, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.devices[id]
	return d, ok
}

// Observation returns a stored observation by id.
func (s *MemStore) Observation(id string) (domain.Observation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.observation[id]
	return o, ok
}

// --- automation ---------------------------------------------------------------

func (s *MemStore) enqueueLocked(msgs []OutboxMessage) {
	for _, m := range msgs {
		s.nextOutbox++
		m.ID = s.nextOutbox
		s.outbox = append(s.outbox, outboxRow{msg: m})
	}
}

func (s *MemStore) InsertAutomationRequest(_ context.Context, req domain.AutomationRequest, outbox ...OutboxMessage) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.autoReq[req.RequestID]; ok {
		return false, nil
	}
	r := req
	s.autoReq[req.RequestID] = &r
	s.autoOrder = append(s.autoOrder, req.RequestID)
	s.enqueueLocked(outbox)
	return true, nil
}

func (s *MemStore) DueAutomation(_ context.Context, now time.Time, lease time.Duration, limit int) ([]domain.AutomationRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.AutomationRequest
	for _, id := range s.autoOrder {
		r := s.autoReq[id]
		due := false
		switch r.Status {
		case domain.AutomationPending:
			due = !r.NotBefore.After(now)
		case domain.AutomationApproved:
			due = true
		case domain.AutomationExecuting:
			due = s.autoClaim[id].Add(lease).Before(now)
		}
		if due {
			out = append(out, *r)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (s *MemStore) ClaimAutomation(_ context.Context, id string, now time.Time, lease time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.autoReq[id]
	if !ok {
		return false, nil
	}
	claimable := r.Status == domain.AutomationPending || r.Status == domain.AutomationApproved ||
		(r.Status == domain.AutomationExecuting && s.autoClaim[id].Add(lease).Before(now))
	if !claimable {
		return false, nil
	}
	r.Status, s.autoClaim[id] = domain.AutomationExecuting, now
	return true, nil
}

func (s *MemStore) SetAutomationStatus(_ context.Context, id string, from []domain.AutomationStatus, to domain.AutomationStatus) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.autoReq[id]
	if !ok {
		return false, nil
	}
	for _, f := range from {
		if r.Status == f {
			r.Status = to
			return true, nil
		}
	}
	return false, nil
}

func (s *MemStore) ApproveAutomation(_ context.Context, id, by string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.autoReq[id]
	if !ok || r.Status != domain.AutomationAwaitingApproval {
		return false, nil
	}
	r.Status, r.ApprovedBy = domain.AutomationApproved, by
	return true, nil
}

func (s *MemStore) CompleteAutomation(_ context.Context, res domain.AutomationResult, outbox ...OutboxMessage) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.autoReq[res.RequestID]
	if !ok {
		return false, domain.Errorf(domain.CategoryPermanent, "unknown automation request %s", res.RequestID)
	}
	if _, done := s.autoRes[res.RequestID]; done {
		return false, nil
	}
	s.autoRes[res.RequestID] = res
	r.Status = res.Status
	s.enqueueLocked(outbox)
	return true, nil
}

func (s *MemStore) AwaitingApprovalBefore(_ context.Context, cutoff time.Time) ([]domain.AutomationRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.AutomationRequest
	for _, id := range s.autoOrder {
		if r := s.autoReq[id]; r.Status == domain.AutomationAwaitingApproval && r.CreatedAt.Before(cutoff) {
			out = append(out, *r)
		}
	}
	return out, nil
}

func executed(st domain.AutomationStatus) bool {
	return st == domain.AutomationSucceeded || st == domain.AutomationFailed || st == domain.AutomationDryRun
}

func (s *MemStore) AutomationHistory(_ context.Context, policyID, deviceID, target string, since time.Time) ([]domain.AutomationRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.AutomationRequest
	for _, id := range s.autoOrder {
		r := s.autoReq[id]
		res, done := s.autoRes[id]
		if r.PolicyID == policyID && r.DeviceID == deviceID && r.Target == target && executed(r.Status) && done && !res.FinishedAt.Before(since) {
			out = append(out, *r)
		}
	}
	return out, nil
}

func (s *MemStore) AttemptsForAlert(_ context.Context, policyID, alertKey string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.autoReq {
		if r.PolicyID == policyID && r.AlertKey == alertKey && executed(r.Status) {
			n++
		}
	}
	return n, nil
}

func (s *MemStore) AlertFiring(_ context.Context, alertKey string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.alerts {
		if a.AlertKey == alertKey && a.Status == domain.AlertFiring {
			return true, nil
		}
	}
	return false, nil
}

func (s *MemStore) GetAutomation(_ context.Context, id string) (*domain.AutomationRequest, *domain.AutomationResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.autoReq[id]
	if !ok {
		return nil, nil, nil
	}
	req := *r
	var res *domain.AutomationResult
	if x, done := s.autoRes[id]; done {
		res = &x
	}
	return &req, res, nil
}

// AutomationRequests returns all requests in insertion order (tests).
func (s *MemStore) AutomationRequests() []domain.AutomationRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.AutomationRequest, 0, len(s.autoOrder))
	for _, id := range s.autoOrder {
		out = append(out, *s.autoReq[id])
	}
	return out
}

// --- reader -------------------------------------------------------------------

func (s *MemStore) DeviceStates(_ context.Context, deviceID string) ([]domain.StateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.StateRecord
	for _, r := range s.states {
		if r.DeviceID == deviceID {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// page applies keyset pagination to items already sorted newest-first.
func pageOf[T any](items []T, key func(T) (time.Time, string), p Page) ([]T, string, error) {
	ct, cid, err := DecodeCursor(p.Cursor)
	if err != nil {
		return nil, "", err
	}
	limit := clampLimit(p.Limit)
	var out []T
	for _, it := range items {
		t, id := key(it)
		if p.Cursor != "" && !(t.Before(ct) || (t.Equal(ct) && id < cid)) {
			continue
		}
		out = append(out, it)
		if len(out) == limit+1 {
			break
		}
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		t, id := key(out[len(out)-1])
		next = EncodeCursor(t, id)
	}
	return out, next, nil
}

func (s *MemStore) ListEvents(_ context.Context, f EventFilter, p Page) ([]domain.Event, string, error) {
	s.mu.Lock()
	var all []domain.Event
	for _, id := range s.evOrder {
		e := s.events[id]
		if (f.DeviceID != "" && e.DeviceID != f.DeviceID) || (f.Type != "" && e.Type != f.Type) ||
			(f.MinSeverity != "" && e.Severity.Rank() < f.MinSeverity.Rank()) ||
			(!f.Since.IsZero() && e.OccurredAt.Before(f.Since)) || (!f.Until.IsZero() && !e.OccurredAt.Before(f.Until)) {
			continue
		}
		all = append(all, e)
	}
	s.mu.Unlock()
	sort.SliceStable(all, func(i, j int) bool {
		if !all[i].OccurredAt.Equal(all[j].OccurredAt) {
			return all[i].OccurredAt.After(all[j].OccurredAt)
		}
		return all[i].EventID > all[j].EventID
	})
	return pageOf(all, func(e domain.Event) (time.Time, string) { return e.OccurredAt, e.EventID }, p)
}

func (s *MemStore) ListAlerts(_ context.Context, f AlertFilter, p Page) ([]domain.Alert, string, error) {
	s.mu.Lock()
	var all []domain.Alert
	for _, a := range s.alerts {
		if (f.Status != "" && a.Status != f.Status) || (f.DeviceID != "" && a.DeviceID != f.DeviceID) {
			continue
		}
		all = append(all, a)
	}
	s.mu.Unlock()
	sort.SliceStable(all, func(i, j int) bool {
		if !all[i].OpenedAt.Equal(all[j].OpenedAt) {
			return all[i].OpenedAt.After(all[j].OpenedAt)
		}
		return all[i].OpenEventID > all[j].OpenEventID
	})
	return pageOf(all, func(a domain.Alert) (time.Time, string) { return a.OpenedAt, a.OpenEventID }, p)
}

func (s *MemStore) ListAutomation(_ context.Context, f AutomationFilter, p Page) ([]AutomationView, string, error) {
	s.mu.Lock()
	var all []AutomationView
	for _, id := range s.autoOrder {
		r := s.autoReq[id]
		if (f.Status != "" && r.Status != f.Status) || (f.DeviceID != "" && r.DeviceID != f.DeviceID) || (f.PolicyID != "" && r.PolicyID != f.PolicyID) {
			continue
		}
		v := AutomationView{Request: *r}
		if res, ok := s.autoRes[id]; ok {
			c := res
			v.Result = &c
		}
		all = append(all, v)
	}
	s.mu.Unlock()
	sort.SliceStable(all, func(i, j int) bool {
		if !all[i].Request.CreatedAt.Equal(all[j].Request.CreatedAt) {
			return all[i].Request.CreatedAt.After(all[j].Request.CreatedAt)
		}
		return all[i].Request.RequestID > all[j].Request.RequestID
	})
	return pageOf(all, func(v AutomationView) (time.Time, string) { return v.Request.CreatedAt, v.Request.RequestID }, p)
}

func (s *MemStore) Metrics(_ context.Context, deviceID, metric string, since, until time.Time, limit int) ([]MetricPoint, error) {
	s.mu.Lock()
	var all []MetricPoint
	for _, id := range s.obsOrder {
		o := s.observation[id]
		if o.DeviceID != deviceID || o.Metric != metric || (!since.IsZero() && o.ObservedAt.Before(since)) || (!until.IsZero() && !o.ObservedAt.Before(until)) {
			continue
		}
		all = append(all, MetricPoint{ObservationID: o.ObservationID, Metric: o.Metric, Value: o.Value, Labels: o.Labels, ObservedAt: o.ObservedAt})
	}
	s.mu.Unlock()
	sort.SliceStable(all, func(i, j int) bool { return all[i].ObservedAt.After(all[j].ObservedAt) })
	if n := clampLimit(limit); len(all) > n {
		all = all[:n]
	}
	return all, nil
}

func (s *MemStore) LatestMetrics(_ context.Context, deviceID string) ([]MetricPoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	best := map[string]MetricPoint{}
	for _, id := range s.obsOrder {
		o := s.observation[id]
		if o.DeviceID != deviceID {
			continue
		}
		k := o.Metric + "|" + domain.LabelsKey(o.Labels)
		if cur, ok := best[k]; !ok || o.ObservedAt.After(cur.ObservedAt) {
			best[k] = MetricPoint{ObservationID: o.ObservationID, Metric: o.Metric, Value: o.Value, Labels: o.Labels, ObservedAt: o.ObservedAt}
		}
	}
	out := make([]MetricPoint, 0, len(best))
	for _, p := range best {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Metric != out[j].Metric {
			return out[i].Metric < out[j].Metric
		}
		return domain.LabelsKey(out[i].Labels) < domain.LabelsKey(out[j].Labels)
	})
	return out, nil
}
