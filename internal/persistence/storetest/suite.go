// Package storetest is the contract suite every persistence.Store must pass.
// Running it against both the in-memory and PostgreSQL stores is what lets the
// rest of the test suite use the fast double with confidence.
package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/persistence"
)

// Factory creates a fresh, empty, migrated store.
type Factory func(t *testing.T) persistence.Store

var t0 = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

func obs(id string) domain.Observation {
	return domain.Observation{
		ObservationID: id, CorrelationID: "c-" + id, DeviceID: "sw1", Source: "snmp", Metric: "network.interface.operational",
		Value: true, Labels: map[string]string{"interface": "Gi0/1"}, ObservedAt: t0, ReceivedAt: t0.Add(time.Second),
		Metadata: map[string]string{"site": "lab"},
	}
}

func event(id, key string, resolves bool) domain.Event {
	return domain.Event{
		EventID: id, CorrelationID: "c", DeviceID: "sw1", Type: "interface.down", Severity: domain.SeverityWarning,
		Message: "down", Labels: map[string]string{"interface": "Gi0/1"}, OccurredAt: t0, ObservationID: "o", Source: "iface",
		AlertKey: key, Resolves: resolves,
	}
}

// Run executes the suite.
func Run(t *testing.T, newStore Factory) {
	ctx := context.Background()
	cases := map[string]func(*testing.T, persistence.Store){
		"ObservationInsertIsIdempotent": func(t *testing.T, s persistence.Store) {
			for i, want := range []bool{true, false, false} {
				err := s.Do(ctx, func(tx persistence.Tx) error {
					got, err := tx.InsertObservation(ctx, obs("o1"))
					if got != want {
						t.Errorf("insert #%d: inserted=%v, want %v", i, got, want)
					}
					return err
				})
				if err != nil {
					t.Fatal(err)
				}
			}
		},
		"ErrorRollsBackEverything": func(t *testing.T, s persistence.Store) {
			boom := errors.New("boom")
			err := s.Do(ctx, func(tx persistence.Tx) error {
				if _, err := tx.InsertObservation(ctx, obs("o1")); err != nil {
					return err
				}
				if err := tx.PutState(ctx, domain.StateRecord{Key: "k", DeviceID: "sw1", DefinitionID: "d", State: domain.StateUp, LastObservedAt: t0, LastSeenAt: t0, LastTransitionAt: t0}); err != nil {
					return err
				}
				if _, err := tx.InsertEvent(ctx, event("e1", "k", false)); err != nil {
					return err
				}
				if _, err := tx.OpenAlert(ctx, event("e1", "k", false)); err != nil {
					return err
				}
				if err := tx.Enqueue(ctx, persistence.OutboxMessage{Subject: "s", MsgID: "m", Payload: []byte("x")}); err != nil {
					return err
				}
				return boom
			})
			if !errors.Is(err, boom) && err == nil {
				t.Fatalf("expected the handler error, got %v", err)
			}
			states, _ := s.ListStates(ctx)
			if len(states) != 0 {
				t.Fatal("state survived a rolled-back transaction")
			}
			n, _ := s.DrainOutbox(ctx, 10, func(context.Context, []persistence.OutboxMessage) error { return nil })
			if n != 0 {
				t.Fatal("outbox message survived a rolled-back transaction")
			}
			_ = s.Do(ctx, func(tx persistence.Tx) error {
				if ins, _ := tx.InsertObservation(ctx, obs("o1")); !ins {
					t.Error("observation survived rollback: a retry would be wrongly treated as a duplicate")
				}
				if ins, _ := tx.InsertEvent(ctx, event("e1", "k", false)); !ins {
					t.Error("event survived rollback")
				}
				if opened, _ := tx.OpenAlert(ctx, event("e1", "k", false)); !opened {
					t.Error("alert survived rollback")
				}
				return nil
			})
		},
		"StateRoundTrip": func(t *testing.T, s persistence.Store) {
			rec := domain.StateRecord{
				Key: "iface/sw1/interface=Gi0/1", DeviceID: "sw1", DefinitionID: "iface", Labels: map[string]string{"interface": "Gi0/1"},
				State: domain.StateSuspect, Failures: 2, Successes: 1, Breached: true, BadSince: t0, GoodSince: t0.Add(time.Minute),
				LastObservedAt: t0.Add(2 * time.Minute), LastSeenAt: t0.Add(3 * time.Minute), LastObservationID: "o9",
				LastTransitionAt: t0.Add(4 * time.Minute), LastValue: "true",
			}
			if err := s.Do(ctx, func(tx persistence.Tx) error {
				if got, _ := tx.GetState(ctx, rec.Key); got != nil {
					t.Error("unknown key must return nil")
				}
				return tx.PutState(ctx, rec)
			}); err != nil {
				t.Fatal(err)
			}
			var got *domain.StateRecord
			_ = s.Do(ctx, func(tx persistence.Tx) error { got, _ = tx.GetState(ctx, rec.Key); return nil })
			if got == nil {
				t.Fatal("state not found")
			}
			if got.State != rec.State || got.Failures != 2 || got.Successes != 1 || !got.Breached || got.Labels["interface"] != "Gi0/1" ||
				!got.BadSince.Equal(rec.BadSince) || !got.GoodSince.Equal(rec.GoodSince) || !got.LastSeenAt.Equal(rec.LastSeenAt) ||
				!got.LastObservedAt.Equal(rec.LastObservedAt) || !got.LastTransitionAt.Equal(rec.LastTransitionAt) ||
				got.LastObservationID != "o9" || got.LastValue != "true" {
				t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, rec)
			}
			// update replaces
			rec.State, rec.Failures = domain.StateDown, 3
			_ = s.Do(ctx, func(tx persistence.Tx) error { return tx.PutState(ctx, rec) })
			list, _ := s.ListStates(ctx)
			if len(list) != 1 || list[0].State != domain.StateDown {
				t.Fatalf("ListStates: %+v", list)
			}
		},
		"EventsAndAlertsLifecycle": func(t *testing.T, s persistence.Store) {
			_ = s.Do(ctx, func(tx persistence.Tx) error {
				ins, _ := tx.InsertEvent(ctx, event("e1", "k", false))
				dup, _ := tx.InsertEvent(ctx, event("e1", "k", false))
				if !ins || dup {
					t.Errorf("event idempotency: %v %v", ins, dup)
				}
				if opened, _ := tx.OpenAlert(ctx, event("e1", "k", false)); !opened {
					t.Error("first open should open")
				}
				if opened, _ := tx.OpenAlert(ctx, event("e2", "k", false)); opened {
					t.Error("a second opening event for a firing alert must not open another")
				}
				if r, _ := tx.ResolveAlert(ctx, event("e3", "other", true)); r {
					t.Error("resolving an unknown alert must be a no-op")
				}
				if r, _ := tx.ResolveAlert(ctx, event("e3", "k", true)); !r {
					t.Error("resolve should close the firing alert")
				}
				if r, _ := tx.ResolveAlert(ctx, event("e4", "k", true)); r {
					t.Error("resolving twice must be a no-op")
				}
				if opened, _ := tx.OpenAlert(ctx, event("e5", "k", false)); !opened {
					t.Error("after resolution the alert can fire again")
				}
				return nil
			})
		},
		"SamplesWindow": func(t *testing.T, s persistence.Store) {
			mk := func(id string, at time.Time, v any, dev string) domain.Observation {
				o := obs(id)
				o.Value, o.ObservedAt, o.DeviceID = v, at, dev
				return o
			}
			_ = s.Do(ctx, func(tx persistence.Tx) error {
				for i, v := range []any{true, false, true, false} {
					if _, err := tx.InsertObservation(ctx, mk(string(rune('a'+i)), t0.Add(time.Duration(i)*time.Minute), v, "sw1")); err != nil {
						return err
					}
				}
				_, _ = tx.InsertObservation(ctx, mk("z", t0.Add(time.Minute), true, "other-device"))
				o := mk("n", t0.Add(30*time.Second), 0.5, "sw1")
				o.Metric = "other.metric"
				_, _ = tx.InsertObservation(ctx, o)
				return nil
			})
			_ = s.Do(ctx, func(tx persistence.Tx) error {
				key := domain.LabelsKey(map[string]string{"interface": "Gi0/1"})
				got, err := tx.Samples(ctx, "sw1", "network.interface.operational", key, t0, t0.Add(3*time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				// from is exclusive, to is inclusive; other devices and metrics are excluded
				if len(got) != 3 || !got[0].At.Equal(t0.Add(time.Minute)) || got[0].Bool == nil || *got[0].Bool != false || got[0].Num != 0 {
					t.Fatalf("samples: %+v", got)
				}
				if got[1].Num != 1 || got[2].At != got[2].At {
					t.Fatalf("ordering/values: %+v", got)
				}
				capped, _ := tx.Samples(ctx, "sw1", "network.interface.operational", key, t0.Add(-time.Hour), t0.Add(time.Minute))
				if len(capped) != 2 {
					t.Fatalf("upper bound must be inclusive and enforced: %+v", capped)
				}
				return nil
			})
		},
		"TransitionInsertIsIdempotent": func(t *testing.T, s persistence.Store) {
			tr := domain.Transition{TransitionID: "t1", Key: "k", DeviceID: "sw1", DefinitionID: "d", From: domain.StateUp, To: domain.StateSuspect, At: t0, ObservationID: "o", CorrelationID: "c"}
			for i := 0; i < 2; i++ {
				if err := s.Do(ctx, func(tx persistence.Tx) error { return tx.InsertTransition(ctx, tr) }); err != nil {
					t.Fatal(err)
				}
			}
		},
		"DevicesAndTouch": func(t *testing.T, s persistence.Store) {
			d := domain.Device{ID: "sw1", Hostname: "sw1", ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Enabled: true,
				Tags: []string{"core"}, Attributes: map[string]string{"a": "b"}, Collection: domain.CollectionSpec{Protocol: "snmp", Profile: "p"}}
			if err := s.UpsertDevices(ctx, []domain.Device{d}); err != nil {
				t.Fatal(err)
			}
			_ = s.Do(ctx, func(tx persistence.Tx) error { return tx.TouchDevice(ctx, "sw1", t0) })
			_ = s.Do(ctx, func(tx persistence.Tx) error { return tx.TouchDevice(ctx, "sw1", t0.Add(-time.Hour)) })
			_ = s.Do(ctx, func(tx persistence.Tx) error { return tx.TouchDevice(ctx, "unknown", t0) })
			d.Hostname = "renamed"
			if err := s.UpsertDevices(ctx, []domain.Device{d}); err != nil {
				t.Fatal(err)
			}
			list, err := s.ListDevices(ctx)
			if err != nil || len(list) != 1 {
				t.Fatalf("%v %v", list, err)
			}
			got := list[0]
			if got.Hostname != "renamed" || !got.LastSeen.Equal(t0) || got.Tags[0] != "core" || got.Attributes["a"] != "b" ||
				got.Collection.Profile != "p" || got.DeviceType != domain.DeviceSwitch {
				t.Fatalf("device round trip: %+v", got)
			}
		},
		"OutboxDelivery": func(t *testing.T, s persistence.Store) {
			for _, id := range []string{"a", "b", "c"} {
				_ = s.Do(ctx, func(tx persistence.Tx) error {
					return tx.Enqueue(ctx, persistence.OutboxMessage{Subject: "s." + id, MsgID: id, Payload: []byte(id), Headers: map[string]string{"h": id}})
				})
			}
			fail := errors.New("nats down")
			if n, err := s.DrainOutbox(ctx, 10, func(context.Context, []persistence.OutboxMessage) error { return fail }); n != 0 || err == nil {
				t.Fatalf("failed publish must keep messages queued: %d %v", n, err)
			}
			var first []persistence.OutboxMessage
			n, err := s.DrainOutbox(ctx, 2, func(_ context.Context, m []persistence.OutboxMessage) error { first = m; return nil })
			if err != nil || n != 2 || first[0].MsgID != "a" || first[1].MsgID != "b" || first[0].Headers["h"] != "a" || string(first[0].Payload) != "a" {
				t.Fatalf("first drain: %d %v %+v", n, err, first)
			}
			var rest []persistence.OutboxMessage
			n, _ = s.DrainOutbox(ctx, 10, func(_ context.Context, m []persistence.OutboxMessage) error { rest = m; return nil })
			if n != 1 || rest[0].MsgID != "c" {
				t.Fatalf("published messages must not be delivered again: %d %+v", n, rest)
			}
			if n, _ = s.DrainOutbox(ctx, 10, func(context.Context, []persistence.OutboxMessage) error { t.Error("nothing left"); return nil }); n != 0 {
				t.Fatal("empty outbox")
			}
		},
		"ConcurrentStateUpdatesSerialisePerKey": func(t *testing.T, s persistence.Store) {
			const workers = 12
			var wg sync.WaitGroup
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := s.Do(ctx, func(tx persistence.Tx) error {
						rec, err := tx.GetState(ctx, "k")
						if err != nil {
							return err
						}
						if rec == nil {
							rec = &domain.StateRecord{Key: "k", DeviceID: "sw1", DefinitionID: "d", State: domain.StateUp, LastObservedAt: t0, LastSeenAt: t0, LastTransitionAt: t0}
						}
						rec.Failures++
						return tx.PutState(ctx, *rec)
					})
					if err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			list, _ := s.ListStates(ctx)
			if len(list) != 1 || list[0].Failures != workers {
				t.Fatalf("lost updates: %+v (want failures=%d)", list, workers)
			}
		},
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) { fn(t, newStore(t)) })
	}
}
