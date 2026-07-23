package state

import (
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

var t0 = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

func f64(v float64) *float64 { return &v }
func bp(v bool) *bool        { return &v }

func reachability() Definition {
	return Definition{
		ID: "device-reachability", Metric: "system.reachable", GoodWhen: &GoodWhen{Equals: bp(true)},
		FailAfter: 3, RecoverAfter: 2, Stale: true,
		Alert:    EventSpec{Event: "device.down", Severity: domain.SeverityCritical, Message: "{device} is unreachable"},
		Recovery: EventSpec{Event: "device.up", Severity: domain.SeverityInfo, Message: "{device} is reachable again"},
	}
}

func cpu() Definition {
	return Definition{
		ID: "cpu-high", Metric: "system.cpu.utilization", KeyLabels: []string{"cpu"},
		Threshold: &Threshold{BreachAbove: f64(0.9), ClearBelow: f64(0.8)},
		FailAfter: 3, RecoverAfter: 3,
		Alert:    EventSpec{Event: "cpu.high", Severity: domain.SeverityWarning, Message: "{device} CPU {value}"},
		Recovery: EventSpec{Event: "cpu.normal", Severity: domain.SeverityInfo},
	}
}

// seq feeds samples one minute apart and returns each update.
type seq struct {
	def  Definition
	rec  *domain.StateRecord
	n    int
	dev  string
	lbl  map[string]string
	upds []Update
}

func newSeq(def Definition) *seq { return &seq{def: def, dev: "sw1"} }

func (s *seq) feed(v any) Update {
	s.n++
	o := domain.Observation{
		ObservationID: domain.StableID("o", s.dev, string(rune(s.n))), CorrelationID: "corr", DeviceID: s.dev,
		Source: "snmp", Metric: s.def.Metric, Value: v, Labels: s.lbl, ObservedAt: t0.Add(time.Duration(s.n) * time.Minute),
	}
	u := Evaluate(s.def, s.rec, o)
	if u.Ignored == NotIgnored {
		rec := u.Record
		s.rec = &rec
	}
	s.upds = append(s.upds, u)
	return u
}

func (s *seq) events() []domain.Event {
	var out []domain.Event
	for _, u := range s.upds {
		out = append(out, u.Events...)
	}
	return out
}

func TestSingleFailedSampleNeverAlerts(t *testing.T) {
	s := newSeq(reachability())
	s.feed(true)
	if u := s.feed(false); u.Record.State != domain.StateSuspect {
		t.Fatalf("one failure should only make the device SUSPECT, got %s", u.Record.State)
	}
	if u := s.feed(true); u.Record.State != domain.StateUp {
		t.Fatalf("a good sample must clear suspicion, got %s", u.Record.State)
	}
	if n := len(s.events()); n != 0 {
		t.Fatalf("no events expected for an isolated failure, got %d", n)
	}
}

func TestFullLifecycleUpSuspectDownRecoveringUp(t *testing.T) {
	s := newSeq(reachability())
	s.feed(true)
	want := []struct {
		v     bool
		state domain.HealthState
		event string
	}{
		{false, domain.StateSuspect, ""},
		{false, domain.StateSuspect, ""},
		{false, domain.StateDown, "device.down"}, // third consecutive failure
		{false, domain.StateDown, ""},            // still down: no repeat alert
		{true, domain.StateRecovering, ""},
		{true, domain.StateUp, "device.up"},
	}
	for i, w := range want {
		u := s.feed(w.v)
		if u.Record.State != w.state {
			t.Fatalf("step %d: state %s, want %s", i, u.Record.State, w.state)
		}
		got := ""
		if len(u.Events) == 1 {
			got = u.Events[0].Type
		}
		if got != w.event {
			t.Fatalf("step %d: event %q, want %q", i, got, w.event)
		}
	}
	evs := s.events()
	if len(evs) != 2 || evs[0].Resolves || !evs[1].Resolves || evs[0].AlertKey != evs[1].AlertKey {
		t.Fatalf("recovery must resolve the alert it belongs to: %+v", evs)
	}
	if evs[0].Severity != domain.SeverityCritical || evs[0].Message != "sw1 is unreachable" || evs[0].CorrelationID != "corr" {
		t.Fatalf("event content: %+v", evs[0])
	}
}

func TestFlappingDuringRecoveryDoesNotReAlert(t *testing.T) {
	s := newSeq(reachability())
	s.feed(true)
	for i := 0; i < 3; i++ {
		s.feed(false)
	}
	s.feed(true) // RECOVERING
	if u := s.feed(false); u.Record.State != domain.StateDown || len(u.Events) != 0 {
		t.Fatalf("RECOVERING -> DOWN must not fire a second alert: %+v", u)
	}
	if n := len(s.events()); n != 1 {
		t.Fatalf("expected exactly the original alert, got %d events", n)
	}
}

func TestFirstSampleAdoptsWithoutAlerting(t *testing.T) {
	s := newSeq(reachability())
	u := s.feed(true)
	if u.Record.State != domain.StateUp || len(u.Events) != 0 || len(u.Transitions) != 1 || u.Transitions[0].From != "" {
		t.Fatalf("adoption: %+v", u)
	}
	// A device first seen failing is not instantly DOWN either.
	s2 := newSeq(reachability())
	if u := s2.feed(false); u.Record.State != domain.StateSuspect || len(u.Events) != 0 {
		t.Fatalf("first bad sample: %+v", u)
	}
}

func TestFailAfterOneAlertsImmediately(t *testing.T) {
	d := reachability()
	d.FailAfter, d.RecoverAfter = 1, 1
	s := newSeq(d)
	s.feed(true)
	if u := s.feed(false); u.Record.State != domain.StateDown || len(u.Events) != 1 {
		t.Fatalf("%+v", u)
	}
	if u := s.feed(true); u.Record.State != domain.StateUp || len(u.Events) != 1 || !u.Events[0].Resolves {
		t.Fatalf("%+v", u)
	}
}

func TestThresholdHysteresis(t *testing.T) {
	s := newSeq(cpu())
	s.feed(0.5)
	// between the lines from below: not a breach
	if u := s.feed(0.85); u.Record.Breached || u.Record.State != domain.StateUp {
		t.Fatalf("0.85 is inside the band and must not breach: %+v", u.Record)
	}
	for i := 0; i < 3; i++ {
		s.feed(0.95)
	}
	if s.rec.State != domain.StateDown || len(s.events()) != 1 || s.events()[0].Type != "cpu.high" {
		t.Fatalf("3 breaching samples should alert once: %s %v", s.rec.State, s.events())
	}
	// inside the band while breached: still breached, so no recovery progress
	for i := 0; i < 5; i++ {
		s.feed(0.85)
	}
	if s.rec.State != domain.StateDown || !s.rec.Breached {
		t.Fatalf("hysteresis violated: %s breached=%v", s.rec.State, s.rec.Breached)
	}
	for i := 0; i < 3; i++ {
		s.feed(0.7)
	}
	if s.rec.State != domain.StateUp {
		t.Fatalf("expected recovery after 3 samples below the clear line, got %s", s.rec.State)
	}
	evs := s.events()
	if len(evs) != 2 || evs[1].Type != "cpu.normal" || !evs[1].Resolves {
		t.Fatalf("events: %+v", evs)
	}
}

func TestThresholdBelowVariant(t *testing.T) {
	d := cpu()
	d.ID, d.Threshold = "charge-low", &Threshold{BreachBelow: f64(0.2), ClearAbove: f64(0.3)}
	d.FailAfter, d.RecoverAfter = 1, 1
	s := newSeq(d)
	s.feed(0.9)
	s.feed(0.1)
	if s.rec.State != domain.StateDown {
		t.Fatal("below breach_below must alert")
	}
	s.feed(0.25)
	if s.rec.State != domain.StateDown {
		t.Fatal("0.25 is below clear_above: still breached")
	}
	s.feed(0.4)
	if s.rec.State != domain.StateUp {
		t.Fatal("0.4 clears the breach")
	}
}

func TestDuplicateAndStaleSamplesAreIgnored(t *testing.T) {
	d := reachability()
	d.FailAfter = 2
	s := newSeq(d)
	s.feed(true)
	first := s.feed(false)
	rec := *s.rec

	dup := Evaluate(d, &rec, domain.Observation{ObservationID: rec.LastObservationID, DeviceID: "sw1", Metric: d.Metric, Value: false, ObservedAt: rec.LastObservedAt.Add(time.Hour)})
	if dup.Ignored != Duplicate || len(dup.Events) != 0 {
		t.Fatalf("redelivered observation must be a no-op: %+v", dup)
	}
	old := Evaluate(d, &rec, domain.Observation{ObservationID: "late", DeviceID: "sw1", Metric: d.Metric, Value: false, ObservedAt: rec.LastObservedAt.Add(-time.Minute)})
	if old.Ignored != Stale {
		t.Fatalf("out-of-order sample must be ignored: %+v", old)
	}
	equal := Evaluate(d, &rec, domain.Observation{ObservationID: "same-ts", DeviceID: "sw1", Metric: d.Metric, Value: false, ObservedAt: rec.LastObservedAt})
	if equal.Ignored != Stale {
		t.Fatal("equal timestamps count as stale")
	}
	if rec.Failures != 1 || first.Record.Failures != 1 {
		t.Fatal("ignored samples must not advance counters")
	}
}

func TestEvaluateIsDeterministic(t *testing.T) {
	mk := func() []domain.Event {
		s := newSeq(reachability())
		s.feed(true)
		for i := 0; i < 3; i++ {
			s.feed(false)
		}
		return s.events()
	}
	a, b := mk(), mk()
	if len(a) != 1 || a[0].EventID != b[0].EventID || a[0].EventID == "" {
		t.Fatal("identical input must produce identical event ids (that is what makes replay idempotent)")
	}
}

func TestDebounceWindow(t *testing.T) {
	d := reachability()
	d.FailAfter, d.FailFor = 2, 5*time.Minute
	s := newSeq(d)
	s.feed(true)
	s.feed(false)
	s.feed(false) // count reached, but only 1 minute since first failure
	if s.rec.State != domain.StateSuspect {
		t.Fatalf("fail_for not honoured: %s", s.rec.State)
	}
	for i := 0; i < 4; i++ {
		s.feed(false)
	}
	if s.rec.State != domain.StateDown {
		t.Fatalf("should be DOWN once the streak lasted long enough: %s", s.rec.State)
	}
}

func TestWrongTypeIsIgnoredNotFatal(t *testing.T) {
	if u := newSeq(reachability()).feed("up"); u.Ignored != WrongType {
		t.Fatalf("%+v", u)
	}
	if u := newSeq(cpu()).feed(true); u.Ignored != WrongType {
		t.Fatalf("bool must not be treated as a number: %+v", u)
	}
	if u := newSeq(cpu()).feed(int64(2)); u.Ignored != NotIgnored {
		t.Fatalf("integers are numbers: %+v", u)
	}
}

func TestKeyLabelsKeepEntitiesIndependent(t *testing.T) {
	iface := Definition{
		ID: "interface-operational", Metric: "network.interface.operational", KeyLabels: []string{"interface"},
		GoodWhen: &GoodWhen{Equals: bp(true)}, FailAfter: 1, RecoverAfter: 1,
		Alert:    EventSpec{Event: "interface.down", Severity: domain.SeverityWarning, Message: "{device} {interface} down"},
		Recovery: EventSpec{Event: "interface.up", Severity: domain.SeverityInfo},
	}
	k1, l1 := KeyFor(iface, "sw1", map[string]string{"interface": "Gi0/1", "if_index": "1"})
	k2, _ := KeyFor(iface, "sw1", map[string]string{"interface": "Gi0/2"})
	if k1 == k2 || len(l1) != 1 {
		t.Fatalf("keys %q %q labels %v", k1, k2, l1)
	}
	a := newSeq(iface)
	a.lbl = map[string]string{"interface": "Gi0/1"}
	a.feed(true)
	u := a.feed(false)
	if len(u.Events) != 1 || u.Events[0].Message != "sw1 Gi0/1 down" || u.Events[0].Labels["interface"] != "Gi0/1" {
		t.Fatalf("event: %+v", u.Events)
	}
}

func TestStaleSamplesDriveDeviceDownAndAreIdempotent(t *testing.T) {
	defs, err := Parse([]byte(`
definitions:
  - id: device-reachability
    metric: system.reachable
    good_when: {equals: true}
    fail_after: 2
    recover_after: 1
    stale: true
    alert: {event: device.down, severity: critical, message: "{device} silent"}
    recovery: {event: device.up, severity: info}
`))
	if err != nil {
		t.Fatal(err)
	}
	def, _ := defs.Get("device-reachability")
	last := t0
	rec := domain.StateRecord{Key: "device-reachability/sw1/", DeviceID: "sw1", DefinitionID: def.ID, State: domain.StateUp, LastObservedAt: last, LastSeenAt: last, LastObservationID: "x"}
	now := last.Add(3 * time.Minute)

	if got := StaleSamples(defs, []domain.StateRecord{rec}, last.Add(30*time.Second), 2*time.Minute, 30*time.Second); len(got) != 0 {
		t.Fatal("fresh data is not stale")
	}
	a := StaleSamples(defs, []domain.StateRecord{rec}, now, 2*time.Minute, 30*time.Second)
	b := StaleSamples(defs, []domain.StateRecord{rec}, now.Add(10*time.Second), 2*time.Minute, 30*time.Second)
	if len(a) != 1 || a[0].ObservationID != b[0].ObservationID || a[0].Value != false || a[0].Source != "sweeper" {
		t.Fatalf("same bucket must give the same id: %+v %+v", a, b)
	}
	// driving the engine with successive buckets reaches DOWN
	cur := &rec
	for i := 0; i < 2; i++ {
		obs := StaleSamples(defs, []domain.StateRecord{*cur}, now.Add(time.Duration(i)*30*time.Second), 2*time.Minute, 30*time.Second)
		u := Evaluate(def, cur, obs[0])
		r := u.Record
		cur = &r
	}
	if cur.State != domain.StateDown {
		t.Fatalf("silence should end in DOWN, got %s", cur.State)
	}
	if got := StaleSamples(defs, []domain.StateRecord{*cur}, now.Add(time.Hour), 2*time.Minute, 30*time.Second); len(got) != 0 {
		t.Fatal("entities already DOWN are skipped")
	}
}

func TestDefinitionParsing(t *testing.T) {
	bad := map[string]string{
		"no metric":       "definitions:\n  - {id: a, good_when: {equals: true}, fail_after: 1, recover_after: 1, alert: {event: e, severity: info}, recovery: {event: r, severity: info}}\n",
		"both judges":     "definitions:\n  - {id: a, metric: m, good_when: {equals: true}, threshold: {breach_above: 1, clear_below: 0}, fail_after: 1, recover_after: 1, alert: {event: e, severity: info}, recovery: {event: r, severity: info}}\n",
		"neither":         "definitions:\n  - {id: a, metric: m, fail_after: 1, recover_after: 1, alert: {event: e, severity: info}, recovery: {event: r, severity: info}}\n",
		"inverted band":   "definitions:\n  - {id: a, metric: m, threshold: {breach_above: 1, clear_below: 2}, fail_after: 1, recover_after: 1, alert: {event: e, severity: info}, recovery: {event: r, severity: info}}\n",
		"half band":       "definitions:\n  - {id: a, metric: m, threshold: {breach_above: 1}, fail_after: 1, recover_after: 1, alert: {event: e, severity: info}, recovery: {event: r, severity: info}}\n",
		"zero fail_after": "definitions:\n  - {id: a, metric: m, good_when: {equals: true}, fail_after: 0, recover_after: 1, alert: {event: e, severity: info}, recovery: {event: r, severity: info}}\n",
		"bad severity":    "definitions:\n  - {id: a, metric: m, good_when: {equals: true}, fail_after: 1, recover_after: 1, alert: {event: e, severity: loud}, recovery: {event: r, severity: info}}\n",
		"bad type":        "definitions:\n  - {id: a, metric: m, device_types: [toaster], good_when: {equals: true}, fail_after: 1, recover_after: 1, alert: {event: e, severity: info}, recovery: {event: r, severity: info}}\n",
		"unknown key":     "definitions:\n  - {id: a, wat: 1}\n",
	}
	for name, doc := range bad {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	dup := "definitions:\n  - {id: a, metric: m, good_when: {equals: true}, fail_after: 1, recover_after: 1, alert: {event: e, severity: info}, recovery: {event: r, severity: info}}\n"
	if _, err := Parse([]byte(dup + dup[len("definitions:\n"):])); err == nil {
		t.Error("duplicate ids must be rejected")
	}
}

func TestShippedDefinitions(t *testing.T) {
	defs, err := LoadFile("../../configs/states.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if got := defs.For("network.interface.operational", domain.DeviceSwitch); len(got) != 1 {
		t.Fatalf("switch interfaces: %v", got)
	}
	if got := defs.For("network.interface.operational", domain.DeviceServer); len(got) != 0 {
		t.Fatal("interface tracking is limited to network device types")
	}
	if got := defs.For("system.cpu.utilization", domain.DeviceServer); len(got) != 1 || got[0].ID != "cpu-high" {
		t.Fatalf("cpu: %v", got)
	}
}
