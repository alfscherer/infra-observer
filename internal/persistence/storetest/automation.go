package storetest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/persistence"
)

// AutomationStoreFactory creates a store that also implements AutomationStore.
type AutomationStoreFactory func(t *testing.T) interface {
	persistence.Store
	persistence.AutomationStore
}

func req(id string, at time.Time) domain.AutomationRequest {
	return domain.AutomationRequest{
		RequestID: id, CorrelationID: "corr", PolicyID: "p1", EventID: "e1", AlertKey: "alert-1", DeviceID: "sw1",
		Action: "restart_service", Target: "svc", Params: map[string]string{"service": "x"}, Reason: "because",
		Proposer: "policy", DryRun: true, Status: domain.AutomationPending, NotBefore: at, CreatedAt: at,
	}
}

func outboxMsg(id string) persistence.OutboxMessage {
	return persistence.OutboxMessage{Subject: "automation.request", MsgID: id, Payload: []byte(id)}
}

// RunAutomation executes the automation storage contract.
func RunAutomation(t *testing.T, newStore AutomationStoreFactory) {
	ctx := context.Background()
	now := t0
	cases := map[string]func(*testing.T, interface {
		persistence.Store
		persistence.AutomationStore
	}){
		"InsertIsIdempotentAndEnqueuesOnce": func(t *testing.T, s interface {
			persistence.Store
			persistence.AutomationStore
		}) {
			for i, want := range []bool{true, false, false} {
				got, err := s.InsertAutomationRequest(ctx, req("r1", now), outboxMsg("r1"))
				if err != nil || got != want {
					t.Fatalf("insert #%d: %v %v", i, got, err)
				}
			}
			var n int
			_, _ = s.DrainOutbox(ctx, 10, func(_ context.Context, m []persistence.OutboxMessage) error { n = len(m); return nil })
			if n != 1 {
				t.Fatalf("a duplicate insert must not enqueue a second notification, got %d", n)
			}
			r, res, err := s.GetAutomation(ctx, "r1")
			if err != nil || r == nil || res != nil || r.Params["service"] != "x" || r.Status != domain.AutomationPending || !r.NotBefore.Equal(now) || !r.DryRun {
				t.Fatalf("get: %+v %v %v", r, res, err)
			}
			if r, _, _ := s.GetAutomation(ctx, "missing"); r != nil {
				t.Fatal("unknown id")
			}
		},
		"DueSelection": func(t *testing.T, s interface {
			persistence.Store
			persistence.AutomationStore
		}) {
			mk := func(id string, st domain.AutomationStatus, nb time.Time) {
				r := req(id, now)
				r.Status, r.NotBefore = st, nb
				_, _ = s.InsertAutomationRequest(ctx, r)
			}
			mk("future", domain.AutomationPending, now.Add(time.Hour))
			mk("ready", domain.AutomationPending, now.Add(-time.Minute))
			mk("approved", domain.AutomationApproved, now.Add(time.Hour))
			mk("waiting", domain.AutomationAwaitingApproval, now.Add(-time.Minute))
			mk("done", domain.AutomationSucceeded, now.Add(-time.Minute))
			due, err := s.DueAutomation(ctx, now, 5*time.Minute, 10)
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]bool{}
			for _, r := range due {
				got[r.RequestID] = true
			}
			if len(got) != 2 || !got["ready"] || !got["approved"] {
				t.Fatalf("due = %v, want ready and approved only", got)
			}
			if lim, _ := s.DueAutomation(ctx, now, 5*time.Minute, 1); len(lim) != 1 {
				t.Fatal("limit not honoured")
			}
		},
		"ClaimIsExclusiveAndLeaseExpires": func(t *testing.T, s interface {
			persistence.Store
			persistence.AutomationStore
		}) {
			_, _ = s.InsertAutomationRequest(ctx, req("r1", now))
			var wins atomic.Int32
			var wg sync.WaitGroup
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if ok, err := s.ClaimAutomation(ctx, "r1", now, time.Minute); err != nil {
						t.Error(err)
					} else if ok {
						wins.Add(1)
					}
				}()
			}
			wg.Wait()
			if wins.Load() != 1 {
				t.Fatalf("exactly one worker may claim a request, %d did", wins.Load())
			}
			if due, _ := s.DueAutomation(ctx, now.Add(30*time.Second), time.Minute, 10); len(due) != 0 {
				t.Fatal("a freshly claimed request is not due")
			}
			// the executor died: after the lease the request becomes due again and can be re-claimed
			later := now.Add(2 * time.Minute)
			if due, _ := s.DueAutomation(ctx, later, time.Minute, 10); len(due) != 1 {
				t.Fatalf("stale claim must be recoverable: %+v", due)
			}
			if ok, _ := s.ClaimAutomation(ctx, "r1", later, time.Minute); !ok {
				t.Fatal("re-claim after lease expiry")
			}
			if ok, _ := s.ClaimAutomation(ctx, "missing", now, time.Minute); ok {
				t.Fatal("unknown request")
			}
		},
		"CompleteRecordsResultOnceAndNotifies": func(t *testing.T, s interface {
			persistence.Store
			persistence.AutomationStore
		}) {
			_, _ = s.InsertAutomationRequest(ctx, req("r1", now))
			res := domain.AutomationResult{RequestID: "r1", CorrelationID: "corr", Status: domain.AutomationDryRun, DryRun: true, Message: "would restart",
				Details: map[string]string{"k": "v"}, StartedAt: now, FinishedAt: now.Add(time.Second)}
			ok, err := s.CompleteAutomation(ctx, res, outboxMsg("res-1"))
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			if again, _ := s.CompleteAutomation(ctx, res, outboxMsg("res-1-dup")); again {
				t.Fatal("completing twice must be a no-op")
			}
			r, got, _ := s.GetAutomation(ctx, "r1")
			if r.Status != domain.AutomationDryRun || got == nil || got.Message != "would restart" || got.Details["k"] != "v" || !got.FinishedAt.Equal(res.FinishedAt) || !got.DryRun {
				t.Fatalf("%+v %+v", r, got)
			}
			var ids []string
			_, _ = s.DrainOutbox(ctx, 10, func(_ context.Context, m []persistence.OutboxMessage) error {
				for _, x := range m {
					ids = append(ids, x.MsgID)
				}
				return nil
			})
			if len(ids) != 1 || ids[0] != "res-1" {
				t.Fatalf("exactly one result notification expected, got %v", ids)
			}
			if _, err := s.CompleteAutomation(ctx, domain.AutomationResult{RequestID: "ghost"}); err == nil {
				t.Fatal("result for an unknown request must be an error")
			}
		},
		"ApprovalFlow": func(t *testing.T, s interface {
			persistence.Store
			persistence.AutomationStore
		}) {
			r := req("r1", now)
			r.Status = domain.AutomationAwaitingApproval
			_, _ = s.InsertAutomationRequest(ctx, r)
			if ok, _ := s.ApproveAutomation(ctx, "missing", "alice"); ok {
				t.Fatal("unknown")
			}
			if old, _ := s.AwaitingApprovalBefore(ctx, now.Add(-time.Hour)); len(old) != 0 {
				t.Fatal("not yet old enough")
			}
			if old, _ := s.AwaitingApprovalBefore(ctx, now.Add(time.Hour)); len(old) != 1 {
				t.Fatal("should be listed once past the cutoff")
			}
			if ok, _ := s.ApproveAutomation(ctx, "r1", "alice"); !ok {
				t.Fatal("approve")
			}
			if ok, _ := s.ApproveAutomation(ctx, "r1", "bob"); ok {
				t.Fatal("approving twice must not succeed (and must not overwrite the approver)")
			}
			got, _, _ := s.GetAutomation(ctx, "r1")
			if got.Status != domain.AutomationApproved || got.ApprovedBy != "alice" {
				t.Fatalf("%+v", got)
			}
			if ok, _ := s.SetAutomationStatus(ctx, "r1", []domain.AutomationStatus{domain.AutomationPending}, domain.AutomationDenied); ok {
				t.Fatal("status guard must apply")
			}
			if ok, _ := s.SetAutomationStatus(ctx, "r1", []domain.AutomationStatus{domain.AutomationApproved, domain.AutomationPending}, domain.AutomationDenied); !ok {
				t.Fatal("status change")
			}
		},
		"HistoryAndAttempts": func(t *testing.T, s interface {
			persistence.Store
			persistence.AutomationStore
		}) {
			mk := func(id string, st domain.AutomationStatus, at time.Time, target, alert string) {
				r := req(id, at)
				r.Target, r.AlertKey = target, alert
				_, _ = s.InsertAutomationRequest(ctx, r)
				_, _ = s.CompleteAutomation(ctx, domain.AutomationResult{RequestID: id, CorrelationID: "corr", Status: st, Message: "m", StartedAt: at, FinishedAt: at})
			}
			mk("old", domain.AutomationSucceeded, now.Add(-2*time.Hour), "svc", "alert-1")
			mk("recent", domain.AutomationDryRun, now.Add(-10*time.Minute), "svc", "alert-1")
			mk("denied", domain.AutomationDenied, now.Add(-5*time.Minute), "svc", "alert-1")
			mk("other-target", domain.AutomationSucceeded, now.Add(-5*time.Minute), "other", "alert-1")
			mk("other-alert", domain.AutomationFailed, now.Add(-5*time.Minute), "svc", "alert-2")
			// created long ago but executed just now: cooldown follows execution
			late := req("late", now.Add(-3*time.Hour))
			late.Target = "svc"
			_, _ = s.InsertAutomationRequest(ctx, late)
			_, _ = s.CompleteAutomation(ctx, domain.AutomationResult{RequestID: "late", CorrelationID: "corr", Status: domain.AutomationSucceeded, Message: "m", StartedAt: now.Add(-time.Minute), FinishedAt: now.Add(-time.Minute)})
			h, err := s.AutomationHistory(ctx, "p1", "sw1", "svc", now.Add(-30*time.Minute))
			if err != nil || len(h) != 3 { // recent + other-alert + late (same policy/device/target); denied is not an attempt
				t.Fatalf("history: %+v %v", h, err)
			}
			if n, _ := s.AttemptsForAlert(ctx, "p1", "alert-1"); n != 4 { // old, recent, other-target, late
				t.Fatalf("attempts for alert-1 = %d, want 4 (denied requests are not attempts)", n)
			}
			if n, _ := s.AttemptsForAlert(ctx, "p2", "alert-1"); n != 0 {
				t.Fatal("attempts are per policy")
			}
		},
		"AlertFiring": func(t *testing.T, s interface {
			persistence.Store
			persistence.AutomationStore
		}) {
			if f, _ := s.AlertFiring(ctx, "k"); f {
				t.Fatal("no alert yet")
			}
			_ = s.Do(ctx, func(tx persistence.Tx) error {
				_, _ = tx.InsertEvent(ctx, event("e1", "k", false))
				_, err := tx.OpenAlert(ctx, event("e1", "k", false))
				return err
			})
			if f, _ := s.AlertFiring(ctx, "k"); !f {
				t.Fatal("alert is firing")
			}
			_ = s.Do(ctx, func(tx persistence.Tx) error { _, err := tx.ResolveAlert(ctx, event("e2", "k", true)); return err })
			if f, _ := s.AlertFiring(ctx, "k"); f {
				t.Fatal("alert resolved")
			}
		},
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) { fn(t, newStore(t)) })
	}
}
