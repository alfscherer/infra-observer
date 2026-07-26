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
		events: map[string]domain.Event{},
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
