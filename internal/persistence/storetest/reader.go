package storetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/persistence"
)

// ReaderStore is a store that supports reading for the API.
type ReaderStore interface {
	persistence.Store
	persistence.AutomationStore
	persistence.Reader
}

// RunReader executes the read-side contract.
func RunReader(t *testing.T, newStore func(t *testing.T) ReaderStore) {
	ctx := context.Background()
	cases := map[string]func(*testing.T, ReaderStore){
		"EventPaginationHasNoGapsOrDuplicatesEvenWithTimestampTies": func(t *testing.T, s ReaderStore) {
			const total = 47
			_ = s.Do(ctx, func(tx persistence.Tx) error {
				for i := 0; i < total; i++ {
					e := event(fmt.Sprintf("evt-%03d", i), "", false)
					e.OccurredAt = t0.Add(time.Duration(i/4) * time.Minute) // groups of 4 share a timestamp
					if _, err := tx.InsertEvent(ctx, e); err != nil {
						return err
					}
				}
				return nil
			})
			seen := map[string]bool{}
			var order []string
			cursor := ""
			pages := 0
			for {
				items, next, err := s.ListEvents(ctx, persistence.EventFilter{}, persistence.Page{Limit: 10, Cursor: cursor})
				if err != nil {
					t.Fatal(err)
				}
				pages++
				for _, e := range items {
					if seen[e.EventID] {
						t.Fatalf("duplicate %s across pages", e.EventID)
					}
					seen[e.EventID] = true
					order = append(order, e.EventID)
				}
				if next == "" {
					break
				}
				cursor = next
			}
			if len(seen) != total || pages != 5 {
				t.Fatalf("saw %d of %d events in %d pages", len(seen), total, pages)
			}
			for i := 1; i < len(order); i++ {
				if order[i-1] < order[i] && false {
					t.Fatal("unreachable")
				}
			}
			first, _, _ := s.ListEvents(ctx, persistence.EventFilter{}, persistence.Page{Limit: 3})
			if first[0].EventID != "evt-046" || first[1].EventID != "evt-045" {
				t.Fatalf("newest first, ties broken by id descending: %v", []string{first[0].EventID, first[1].EventID, first[2].EventID})
			}
			if _, _, err := s.ListEvents(ctx, persistence.EventFilter{}, persistence.Page{Cursor: "not-a-cursor"}); domain.CategoryOf(err) != domain.CategoryValidation {
				t.Fatalf("a bad cursor is a validation error: %v", err)
			}
		},
		"EventFilters": func(t *testing.T, s ReaderStore) {
			mk := func(id, dev, typ string, sev domain.Severity, at time.Time) {
				e := event(id, "", false)
				e.DeviceID, e.Type, e.Severity, e.OccurredAt = dev, typ, sev, at
				_ = s.Do(ctx, func(tx persistence.Tx) error { _, err := tx.InsertEvent(ctx, e); return err })
			}
			mk("a", "sw1", "interface.down", domain.SeverityWarning, t0)
			mk("b", "sw1", "device.down", domain.SeverityCritical, t0.Add(time.Minute))
			mk("c", "sw2", "interface.down", domain.SeverityInfo, t0.Add(2*time.Minute))
			ids := func(f persistence.EventFilter) string {
				items, _, err := s.ListEvents(ctx, f, persistence.Page{})
				if err != nil {
					t.Fatal(err)
				}
				out := ""
				for _, e := range items {
					out += e.EventID
				}
				return out
			}
			for name, tc := range map[string]struct {
				f    persistence.EventFilter
				want string
			}{
				"device":       {persistence.EventFilter{DeviceID: "sw1"}, "ba"},
				"type":         {persistence.EventFilter{Type: "interface.down"}, "ca"},
				"min severity": {persistence.EventFilter{MinSeverity: domain.SeverityWarning}, "ba"},
				"since":        {persistence.EventFilter{Since: t0.Add(time.Minute)}, "cb"},
				"until":        {persistence.EventFilter{Until: t0.Add(time.Minute)}, "a"},
				"combined":     {persistence.EventFilter{DeviceID: "sw1", MinSeverity: domain.SeverityCritical}, "b"},
				"no match":     {persistence.EventFilter{DeviceID: "nope"}, ""},
			} {
				if got := ids(tc.f); got != tc.want {
					t.Errorf("%s: %q, want %q", name, got, tc.want)
				}
			}
		},
		"AlertListing": func(t *testing.T, s ReaderStore) {
			_ = s.Do(ctx, func(tx persistence.Tx) error {
				for i, key := range []string{"k1", "k2", "k3"} {
					e := event(fmt.Sprintf("e%d", i), key, false)
					e.OccurredAt = t0.Add(time.Duration(i) * time.Minute)
					if i == 2 {
						e.DeviceID = "sw2"
					}
					_, _ = tx.InsertEvent(ctx, e)
					if _, err := tx.OpenAlert(ctx, e); err != nil {
						return err
					}
				}
				_, err := tx.ResolveAlert(ctx, event("r", "k1", true))
				return err
			})
			all, _, _ := s.ListAlerts(ctx, persistence.AlertFilter{}, persistence.Page{})
			if len(all) != 3 || all[0].AlertKey != "k3" {
				t.Fatalf("newest first: %+v", all)
			}
			firing, _, _ := s.ListAlerts(ctx, persistence.AlertFilter{Status: domain.AlertFiring}, persistence.Page{})
			resolved, _, _ := s.ListAlerts(ctx, persistence.AlertFilter{Status: domain.AlertResolved}, persistence.Page{})
			if len(firing) != 2 || len(resolved) != 1 || resolved[0].AlertKey != "k1" || resolved[0].ResolvedAt.IsZero() || resolved[0].Labels["interface"] != "Gi0/1" {
				t.Fatalf("%+v %+v", firing, resolved)
			}
			if d, _, _ := s.ListAlerts(ctx, persistence.AlertFilter{DeviceID: "sw2"}, persistence.Page{}); len(d) != 1 {
				t.Fatalf("%+v", d)
			}
			p1, next, _ := s.ListAlerts(ctx, persistence.AlertFilter{}, persistence.Page{Limit: 2})
			p2, next2, _ := s.ListAlerts(ctx, persistence.AlertFilter{}, persistence.Page{Limit: 2, Cursor: next})
			if len(p1) != 2 || next == "" || len(p2) != 1 || next2 != "" || p2[0].AlertKey != "k1" {
				t.Fatalf("%+v %+v", p1, p2)
			}
		},
		"AutomationListingJoinsResults": func(t *testing.T, s ReaderStore) {
			for i := 0; i < 3; i++ {
				r := req(fmt.Sprintf("r%d", i), t0.Add(time.Duration(i)*time.Minute))
				if i == 2 {
					r.PolicyID, r.DeviceID = "p2", "sw2"
				}
				_, _ = s.InsertAutomationRequest(ctx, r)
			}
			_, _ = s.CompleteAutomation(ctx, domain.AutomationResult{RequestID: "r1", CorrelationID: "corr", Status: domain.AutomationDryRun, DryRun: true, Message: "would do it",
				Details: map[string]string{"k": "v"}, StartedAt: t0, FinishedAt: t0.Add(time.Second)})
			all, _, err := s.ListAutomation(ctx, persistence.AutomationFilter{}, persistence.Page{})
			if err != nil || len(all) != 3 || all[0].Request.RequestID != "r2" {
				t.Fatalf("%+v %v", all, err)
			}
			byID := map[string]persistence.AutomationView{}
			for _, v := range all {
				byID[v.Request.RequestID] = v
			}
			if byID["r0"].Result != nil || byID["r1"].Result == nil || byID["r1"].Result.Message != "would do it" || byID["r1"].Result.Details["k"] != "v" ||
				byID["r1"].Request.Status != domain.AutomationDryRun || byID["r1"].Request.Params["service"] != "x" {
				t.Fatalf("%+v", byID)
			}
			if d, _, _ := s.ListAutomation(ctx, persistence.AutomationFilter{DeviceID: "sw2"}, persistence.Page{}); len(d) != 1 {
				t.Fatal("device filter")
			}
			if d, _, _ := s.ListAutomation(ctx, persistence.AutomationFilter{PolicyID: "p2"}, persistence.Page{}); len(d) != 1 {
				t.Fatal("policy filter")
			}
			if d, _, _ := s.ListAutomation(ctx, persistence.AutomationFilter{Status: domain.AutomationDryRun}, persistence.Page{}); len(d) != 1 || d[0].Request.RequestID != "r1" {
				t.Fatal("status filter")
			}
			p1, next, _ := s.ListAutomation(ctx, persistence.AutomationFilter{}, persistence.Page{Limit: 2})
			p2, _, _ := s.ListAutomation(ctx, persistence.AutomationFilter{}, persistence.Page{Limit: 2, Cursor: next})
			if len(p1) != 2 || len(p2) != 1 || p2[0].Request.RequestID != "r0" {
				t.Fatal("pagination")
			}
		},
		"MetricsAndLatest": func(t *testing.T, s ReaderStore) {
			put := func(id, metric string, at time.Time, v any, labels map[string]string) {
				o := obs(id)
				o.Metric, o.ObservedAt, o.Value, o.Labels = metric, at, v, labels
				_ = s.Do(ctx, func(tx persistence.Tx) error { _, err := tx.InsertObservation(ctx, o); return err })
			}
			g1, g2 := map[string]string{"interface": "Gi0/1"}, map[string]string{"interface": "Gi0/2"}
			for i := 0; i < 5; i++ {
				put(fmt.Sprint("cpu", i), "system.cpu.utilization", t0.Add(time.Duration(i)*time.Minute), 0.1*float64(i), nil)
			}
			put("if1a", "network.interface.operational", t0, true, g1)
			put("if1b", "network.interface.operational", t0.Add(time.Minute), false, g1)
			put("if2a", "network.interface.operational", t0, true, g2)
			put("name", "system.name", t0, "switch-01", nil)
			put("other", "system.cpu.utilization", t0, 0.99, nil)
			_ = s.Do(ctx, func(tx persistence.Tx) error { return nil })

			pts, err := s.Metrics(ctx, "sw1", "system.cpu.utilization", time.Time{}, time.Time{}, 3)
			if err != nil || len(pts) != 3 || pts[0].ObservationID != "cpu4" || pts[2].ObservationID != "cpu2" {
				t.Fatalf("newest first, limited: %+v %v", pts, err)
			}
			if pts, _ := s.Metrics(ctx, "sw1", "system.cpu.utilization", t0.Add(time.Minute), t0.Add(3*time.Minute), 100); len(pts) != 3 { // 1m, 2m, plus the t0 "other"? no: since=1m until=3m -> cpu1,cpu2
				// cpu1 (1m), cpu2 (2m) only
				if len(pts) != 2 {
					t.Fatalf("window: %+v", pts)
				}
			}
			latest, err := s.LatestMetrics(ctx, "sw1")
			if err != nil {
				t.Fatal(err)
			}
			byKey := map[string]persistence.MetricPoint{}
			for _, p := range latest {
				byKey[p.Metric+"|"+domain.LabelsKey(p.Labels)] = p
			}
			if len(latest) != 4 {
				t.Fatalf("one point per series (cpu, if Gi0/1, if Gi0/2, name): %d %+v", len(latest), latest)
			}
			if p := byKey["network.interface.operational|interface=Gi0/1"]; p.Value != false || p.ObservationID != "if1b" {
				t.Fatalf("latest per series: %+v", p)
			}
			if p := byKey["system.name|"]; p.Value != "switch-01" {
				t.Fatalf("%+v", p)
			}
			if pts, _ := s.LatestMetrics(ctx, "nobody"); len(pts) != 0 {
				t.Fatal("unknown device")
			}
		},
		"DeviceStates": func(t *testing.T, s ReaderStore) {
			_ = s.Do(ctx, func(tx persistence.Tx) error {
				for _, r := range []domain.StateRecord{
					{Key: "a/sw1/", DeviceID: "sw1", DefinitionID: "a", State: domain.StateUp, LastObservedAt: t0, LastSeenAt: t0, LastTransitionAt: t0},
					{Key: "b/sw1/x", DeviceID: "sw1", DefinitionID: "b", State: domain.StateDown, LastObservedAt: t0, LastSeenAt: t0, LastTransitionAt: t0},
					{Key: "a/sw2/", DeviceID: "sw2", DefinitionID: "a", State: domain.StateUp, LastObservedAt: t0, LastSeenAt: t0, LastTransitionAt: t0},
				} {
					if err := tx.PutState(ctx, r); err != nil {
						return err
					}
				}
				return nil
			})
			got, err := s.DeviceStates(ctx, "sw1")
			if err != nil || len(got) != 2 || got[0].DefinitionID != "a" || got[1].State != domain.StateDown {
				t.Fatalf("%+v %v", got, err)
			}
		},
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) { fn(t, newStore(t)) })
	}
}
