package rules

import (
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/state"
)

var t0 = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

func f64(v float64) *float64 { return &v }

func flapRule() Rule {
	return Rule{ID: "flap", Match: Match{Metric: "network.interface.operational"}, Severity: domain.SeverityWarning,
		Condition: Condition{Transitions: 3, Within: 10 * time.Minute}, Event: "interface.flapping", ClearEvent: "interface.stable",
		Message: "{device} {interface} flapped {value}x"}
}

func boolSamples(vals ...bool) []domain.Sample {
	out := make([]domain.Sample, len(vals))
	for i, v := range vals {
		v := v
		n := 0.0
		if v {
			n = 1
		}
		out[i] = domain.Sample{At: t0.Add(time.Duration(i) * time.Minute), Num: n, Bool: &v}
	}
	return out
}

func numSamples(vals ...float64) []domain.Sample {
	out := make([]domain.Sample, len(vals))
	for i, v := range vals {
		out[i] = domain.Sample{At: t0.Add(time.Duration(i) * time.Minute), Num: v}
	}
	return out
}

func ob(n int) domain.Observation {
	return domain.Observation{ObservationID: string(rune('a' + n)), CorrelationID: "corr", DeviceID: "sw1", Metric: "m",
		Labels: map[string]string{"interface": "Gi0/1"}, ObservedAt: t0.Add(time.Duration(n) * time.Minute)}
}

func TestTransitionsRuleFiresOnceAndClears(t *testing.T) {
	r := flapRule()
	// 3 flips: T F T F
	u := Evaluate(r, nil, boolSamples(true, false, true), ob(2))
	if len(u.Events) != 0 || u.Record.State != domain.StateUp {
		t.Fatalf("2 flips must not fire: %+v", u)
	}
	u = Evaluate(r, &u.Record, boolSamples(true, false, true, false), ob(3))
	if len(u.Events) != 1 {
		t.Fatalf("3 flips should fire: %+v", u)
	}
	ev := u.Events[0]
	if ev.Type != "interface.flapping" || ev.Severity != domain.SeverityWarning || ev.Resolves || ev.Message != "sw1 Gi0/1 flapped 3x" ||
		ev.Labels["rule_id"] != "flap" || ev.CorrelationID != "corr" || ev.AlertKey == "" {
		t.Fatalf("event: %+v", ev)
	}
	// still flapping: no repeat
	rec := u.Record
	u = Evaluate(r, &rec, boolSamples(true, false, true, false, true), ob(4))
	if len(u.Events) != 0 || u.Record.State != domain.StateDown {
		t.Fatalf("sustained condition must not re-fire: %+v", u)
	}
	// settled
	rec = u.Record
	u = Evaluate(r, &rec, boolSamples(true, true, true), ob(5))
	if len(u.Events) != 1 || u.Events[0].Type != "interface.stable" || !u.Events[0].Resolves || u.Events[0].AlertKey != ev.AlertKey {
		t.Fatalf("clear: %+v", u)
	}
}

func TestAggregateRules(t *testing.T) {
	avg := Rule{ID: "cpu", Match: Match{Metric: "m"}, Severity: domain.SeverityWarning,
		Condition: Condition{AverageOver: 5 * time.Minute, GreaterThan: f64(0.9), MinSamples: 3}}
	if u := Evaluate(avg, nil, numSamples(0.95, 0.95), ob(1)); len(u.Events) != 0 {
		t.Fatal("too few samples is no data, not a breach")
	}
	if u := Evaluate(avg, nil, numSamples(0.99, 0.5, 0.5), ob(2)); len(u.Events) != 0 {
		t.Fatalf("average 0.66 must not fire: %+v", u)
	}
	u := Evaluate(avg, nil, numSamples(0.99, 0.95, 0.91), ob(2))
	if len(u.Events) != 1 || u.Events[0].Type != "rule.triggered" {
		t.Fatalf("average 0.95 should fire with the default event name: %+v", u)
	}
	if u.Record.LastValue != "0.95" {
		t.Fatalf("measured value: %q", u.Record.LastValue)
	}
	mx := Rule{ID: "t", Match: Match{Metric: "m"}, Severity: domain.SeverityCritical, Condition: Condition{MaxOver: time.Minute, GreaterThan: f64(75), MinSamples: 1}}
	if u := Evaluate(mx, nil, numSamples(60, 80, 61), ob(2)); len(u.Events) != 1 {
		t.Fatalf("max rule: %+v", u)
	}
	mn := Rule{ID: "b", Match: Match{Metric: "m"}, Severity: domain.SeverityCritical, Condition: Condition{MinOver: time.Minute, LessThan: f64(0.2), MinSamples: 2}}
	if u := Evaluate(mn, nil, numSamples(0.5, 0.1), ob(1)); len(u.Events) != 1 {
		t.Fatalf("min rule: %+v", u)
	}
}

func TestEvaluateIgnoresDuplicatesAndStale(t *testing.T) {
	r := flapRule()
	u := Evaluate(r, nil, boolSamples(true, false, true, false), ob(3))
	rec := u.Record
	if d := Evaluate(r, &rec, boolSamples(true), domain.Observation{ObservationID: rec.LastObservationID, DeviceID: "sw1", ObservedAt: t0.Add(time.Hour)}); d.Ignored != state.Duplicate || len(d.Events) != 0 {
		t.Fatal("duplicate must be ignored")
	}
	if d := Evaluate(r, &rec, boolSamples(true), domain.Observation{ObservationID: "old", DeviceID: "sw1", ObservedAt: t0}); d.Ignored != state.Stale {
		t.Fatal("stale must be ignored")
	}
}

func TestEvaluateIsDeterministic(t *testing.T) {
	r := flapRule()
	a := Evaluate(r, nil, boolSamples(true, false, true, false), ob(3))
	b := Evaluate(r, nil, boolSamples(true, false, true, false), ob(3))
	if a.Events[0].EventID != b.Events[0].EventID || a.Events[0].EventID == "" {
		t.Fatal("event ids must be deterministic so replay cannot duplicate events")
	}
}

func TestEntitiesAreIndependentPerLabelSet(t *testing.T) {
	r := flapRule()
	k1, _ := KeyFor(r, "sw1", map[string]string{"interface": "Gi0/1"})
	k2, _ := KeyFor(r, "sw1", map[string]string{"interface": "Gi0/2"})
	k3, _ := KeyFor(r, "sw2", map[string]string{"interface": "Gi0/1"})
	if k1 == k2 || k1 == k3 {
		t.Fatal("each interface of each device needs its own activation")
	}
}

func TestMatching(t *testing.T) {
	rs, err := Parse([]byte(`
rules:
  - id: a
    match: {device_type: [server, workstation], metric: m, site: lab, tags: [prod]}
    condition: {average_over: 1m, greater_than: 1}
    severity: info
  - id: b
    match: {device_type: switch, metric: m}
    condition: {max_over: 1m, greater_than: 1}
    severity: info
`))
	if err != nil {
		t.Fatal(err)
	}
	srv := domain.Device{ID: "s", DeviceType: domain.DeviceServer, Site: "lab", Tags: []string{"prod", "x"}}
	if got := rs.For("m", srv); len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("%v", got)
	}
	srv.Tags = nil
	if got := rs.For("m", srv); len(got) != 0 {
		t.Fatal("missing tag must not match")
	}
	if got := rs.For("other", srv); len(got) != 0 {
		t.Fatal("metric must match")
	}
	var nilRules *Rules
	if len(nilRules.For("m", srv)) != 0 || len(nilRules.All()) != 0 {
		t.Fatal("nil rule set must be a no-op")
	}
}

func TestParseValidation(t *testing.T) {
	head := "rules:\n  - id: r\n    match: {metric: m}\n    severity: warning\n"
	bad := map[string]string{
		"no condition":       head + "    condition: {}\n",
		"two kinds":          head + "    condition: {transitions: 2, within: 1m, average_over: 1m, greater_than: 1}\n",
		"transitions no win": head + "    condition: {transitions: 2}\n",
		"agg no limit":       head + "    condition: {average_over: 1m}\n",
		"agg both limits":    head + "    condition: {average_over: 1m, greater_than: 1, less_than: 0}\n",
		"limit no agg":       head + "    condition: {greater_than: 1}\n",
		"two aggs":           head + "    condition: {average_over: 1m, max_over: 1m, greater_than: 1}\n",
		"bad severity":       "rules:\n  - {id: r, match: {metric: m}, severity: loud, condition: {average_over: 1m, greater_than: 1}}\n",
		"no metric":          "rules:\n  - {id: r, severity: info, condition: {average_over: 1m, greater_than: 1}}\n",
		"bad type":           "rules:\n  - {id: r, match: {metric: m, device_type: toaster}, severity: info, condition: {average_over: 1m, greater_than: 1}}\n",
		"unknown key":        head + "    condition: {average_over: 1m, greater_than: 1}\n    colour: red\n",
	}
	for name, doc := range bad {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	ok := "rules:\n  - {id: r, match: {metric: m}, severity: info, condition: {average_over: 1m, greater_than: 1}}\n"
	if _, err := Parse([]byte(ok + ok[len("rules:\n"):])); err == nil {
		t.Error("duplicate ids must be rejected")
	}
}

func TestShippedRulesLoadAndCoverSpecExamples(t *testing.T) {
	rs, err := LoadFile("../../configs/rules.yaml")
	if err != nil {
		t.Fatal(err)
	}
	sw := domain.Device{ID: "sw", DeviceType: domain.DeviceSwitch}
	if got := rs.For("network.interface.operational", sw); len(got) != 1 || got[0].Condition.Transitions != 5 || got[0].Condition.Within != 10*time.Minute {
		t.Fatalf("flapping rule from the spec: %+v", got)
	}
	srv := domain.Device{ID: "srv", DeviceType: domain.DeviceServer}
	if got := rs.For("system.cpu.utilization", srv); len(got) != 1 || got[0].Condition.AverageOver != 5*time.Minute || *got[0].Condition.GreaterThan != 0.90 {
		t.Fatalf("cpu rule from the spec: %+v", got)
	}
}
