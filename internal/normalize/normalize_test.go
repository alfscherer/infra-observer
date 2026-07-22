package normalize

import (
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/collector/snmp"
	"github.com/alfscherer/infra-observer/internal/domain"
)

func load(t *testing.T) *Normalizer {
	t.Helper()
	n, err := LoadFile("../../configs/normalization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func raw(metric string, value any, labels map[string]string) domain.Observation {
	return domain.Observation{
		ObservationID: "o1", CorrelationID: "c1", DeviceID: "sw1", Source: "snmp",
		Metric: metric, Value: value, Labels: labels, ObservedAt: time.Unix(1700000000, 0),
		Metadata: map[string]string{"oid": "1.2.3"},
	}
}

func TestInterfaceOperStatusBecomesBoolWithCanonicalLabels(t *testing.T) {
	n := load(t)
	up, res, err := n.Normalize(raw("snmp.ifOperStatus", 1.0, map[string]string{"ifIndex": "1", "ifName": "Gi0/1", "junk": "x"}))
	if err != nil || res != Mapped {
		t.Fatal(err, res)
	}
	if up.Metric != "network.interface.operational" || up.Value != true {
		t.Fatalf("got %s=%v", up.Metric, up.Value)
	}
	if up.Labels["interface"] != "Gi0/1" || up.Labels["if_index"] != "1" || len(up.Labels) != 2 {
		t.Fatalf("labels not canonicalised: %v", up.Labels)
	}
	if up.Metadata["raw_metric"] != "snmp.ifOperStatus" || up.Metadata["oid"] != "1.2.3" {
		t.Fatalf("metadata: %v", up.Metadata)
	}
	// identity of the measurement is preserved for idempotency and tracing
	if up.ObservationID != "o1" || up.CorrelationID != "c1" || !up.ObservedAt.Equal(time.Unix(1700000000, 0)) {
		t.Fatalf("envelope changed: %+v", up)
	}
	for _, v := range []float64{2, 3, 5, 7} {
		down, _, _ := n.Normalize(raw("snmp.ifOperStatus", v, map[string]string{"ifName": "Gi0/1"}))
		if down.Value != false {
			t.Errorf("ifOperStatus %v must not be operational", v)
		}
	}
}

func TestScaleAndBoolConversions(t *testing.T) {
	n := load(t)
	cpu, _, err := n.Normalize(raw("snmp.cpuLoadPercent", 95.0, nil))
	if err != nil || cpu.Metric != "system.cpu.utilization" {
		t.Fatal(err, cpu.Metric)
	}
	if f, _ := cpu.Float(); f < 0.9499 || f > 0.9501 {
		t.Fatalf("95%% should become 0.95, got %v", cpu.Value)
	}
	batt, _, _ := n.Normalize(raw("snmp.upsOutputSource", 5.0, nil))
	mains, _, _ := n.Normalize(raw("snmp.upsOutputSource", 3.0, nil))
	if batt.Metric != "power.ups.on_battery" || batt.Value != true || mains.Value != false {
		t.Fatalf("ups source: %v %v", batt.Value, mains.Value)
	}
	uptime, _, _ := n.Normalize(raw("snmp.sysUpTime", 3600.0, nil))
	if uptime.Metric != "system.uptime" || uptime.Value != 3600.0 || uptime.Metadata["unit"] != "seconds" {
		t.Fatalf("uptime: %+v", uptime)
	}
}

func TestUnmappedMetricPassesThroughUnchanged(t *testing.T) {
	n := load(t)
	in := raw("vendor.cpu.load", 40.0, nil)
	out, res, err := n.Normalize(in)
	if err != nil || res != Passthrough || out.Metric != "vendor.cpu.load" || out.Value != 40.0 {
		t.Fatalf("%+v %v %v", out, res, err)
	}
}

func TestNormalizeDoesNotMutateInput(t *testing.T) {
	n := load(t)
	in := raw("snmp.ifOperStatus", 1.0, map[string]string{"ifName": "Gi0/1"})
	_, _, _ = n.Normalize(in)
	if in.Metric != "snmp.ifOperStatus" || in.Labels["ifName"] != "Gi0/1" || in.Metadata["raw_metric"] != "" {
		t.Fatalf("input mutated: %+v", in)
	}
}

func TestBadValuesAreValidationErrors(t *testing.T) {
	n := load(t)
	for _, in := range []domain.Observation{
		raw("snmp.ifOperStatus", "up", nil),
		raw("snmp.cpuLoadPercent", "hot", nil),
	} {
		if _, _, err := n.Normalize(in); domain.CategoryOf(err) != domain.CategoryValidation {
			t.Errorf("%s: %v", in.Metric, err)
		}
	}
}

func TestEnumAndAlreadyBool(t *testing.T) {
	n, err := Parse([]byte(`
mappings:
  - raw: vendor.mode
    metric: vendor.mode.name
    convert: {kind: enum, map: {"1": "auto", "2": "manual"}}
  - raw: vendor.flag
    metric: vendor.flag.set
    convert: {kind: bool, true_values: [1]}
`))
	if err != nil {
		t.Fatal(err)
	}
	if o, _, err := n.Normalize(raw("vendor.mode", 2.0, nil)); err != nil || o.Value != "manual" {
		t.Fatal(o.Value, err)
	}
	if _, _, err := n.Normalize(raw("vendor.mode", 9.0, nil)); err == nil {
		t.Fatal("unknown enum member must be an error")
	}
	if o, _, _ := n.Normalize(raw("vendor.flag", true, nil)); o.Value != true {
		t.Fatal("bool passes through bool")
	}
}

func TestParseRejectsBadMappings(t *testing.T) {
	for name, doc := range map[string]string{
		"no raw":         "mappings:\n  - {metric: a.b}\n",
		"bad canonical":  "mappings:\n  - {raw: x, metric: Not.Canonical}\n",
		"dup":            "mappings:\n  - {raw: x, metric: a.b}\n  - {raw: x, metric: a.c}\n",
		"scale zero":     "mappings:\n  - {raw: x, metric: a.b, convert: {kind: scale}}\n",
		"bool no values": "mappings:\n  - {raw: x, metric: a.b, convert: {kind: bool}}\n",
		"unknown kind":   "mappings:\n  - {raw: x, metric: a.b, convert: {kind: magic}}\n",
		"unknown key":    "mappings:\n  - {raw: x, metric: a.b, colour: red}\n",
	} {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// Every metric a shipped SNMP profile can emit must either be mapped or be a
// deliberate pass-through for a script transform. This catches profile edits
// that silently produce metrics nothing understands.
func TestShippedProfilesAreFullyCovered(t *testing.T) {
	n := load(t)
	ps, err := snmp.LoadProfiles("../../configs/profiles")
	if err != nil {
		t.Fatal(err)
	}
	mapped := map[string]bool{}
	for _, r := range n.RawMetrics() {
		mapped[r] = true
	}
	scriptHandled := map[string]bool{"vendor.cpu.load": true, "vendor.temperature.decic": true}
	for _, id := range ps.IDs() {
		p, _ := ps.Get(id)
		for _, m := range p.Metrics {
			if !mapped[m.Name] && !scriptHandled[m.Name] {
				t.Errorf("profile %s emits %s, which has neither a mapping nor a script transform", id, m.Name)
			}
		}
	}
}
