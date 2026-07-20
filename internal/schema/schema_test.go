package schema

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

var fixedNow = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

func validator() Validator {
	v := NewValidator()
	v.Now = func() time.Time { return fixedNow }
	return v
}

func good() domain.Observation {
	return domain.Observation{
		ObservationID: "o1", CorrelationID: "c1", DeviceID: "sw1", Source: "snmp",
		Metric: "snmp.ifOperStatus", Value: 1.0, Labels: map[string]string{"ifName": "Gi0/1"},
		ObservedAt: fixedNow.Add(-time.Second),
	}
}

func TestValidateAccepts(t *testing.T) {
	if err := validator().Validate(good()); err != nil {
		t.Fatal(err)
	}
	for _, v := range []any{true, "text", 3.5, int64(3), float64(0)} {
		o := good()
		o.Value = v
		if err := validator().Validate(o); err != nil {
			t.Errorf("value %v (%T) rejected: %v", v, v, err)
		}
	}
	old := good()
	old.ObservedAt = fixedNow.Add(-90 * 24 * time.Hour)
	if err := validator().Validate(old); err != nil {
		t.Fatalf("old observations must stay valid so replay works: %v", err)
	}
}

func TestValidateRejects(t *testing.T) {
	long := strings.Repeat("x", 2000)
	many := map[string]string{}
	for i := 0; i < 40; i++ {
		many[string(rune('a'+i%26))+strings.Repeat("k", i)] = "v"
	}
	mutations := map[string]func(*domain.Observation){
		"no id":          func(o *domain.Observation) { o.ObservationID = "" },
		"no correlation": func(o *domain.Observation) { o.CorrelationID = "" },
		"no device":      func(o *domain.Observation) { o.DeviceID = "" },
		"no source":      func(o *domain.Observation) { o.Source = "" },
		"bad metric":     func(o *domain.Observation) { o.Metric = "has space" },
		"empty metric":   func(o *domain.Observation) { o.Metric = "" },
		"nil value":      func(o *domain.Observation) { o.Value = nil },
		"map value":      func(o *domain.Observation) { o.Value = map[string]any{"a": 1} },
		"nan":            func(o *domain.Observation) { o.Value = nanValue() },
		"long string":    func(o *domain.Observation) { o.Value = long },
		"zero time":      func(o *domain.Observation) { o.ObservedAt = time.Time{} },
		"future":         func(o *domain.Observation) { o.ObservedAt = fixedNow.Add(time.Hour) },
		"bad label key":  func(o *domain.Observation) { o.Labels = map[string]string{"bad key": "v"} },
		"long label":     func(o *domain.Observation) { o.Labels = map[string]string{"k": long} },
		"many labels":    func(o *domain.Observation) { o.Labels = many },
	}
	for name, mut := range mutations {
		o := good()
		mut(&o)
		err := validator().Validate(o)
		if err == nil {
			t.Errorf("%s: expected rejection", name)
		} else if domain.CategoryOf(err) != domain.CategoryValidation {
			t.Errorf("%s: category %q, want validation", name, domain.CategoryOf(err))
		}
	}
}

func nanValue() float64 { var z float64; return z / z }

func TestDecodeStrictness(t *testing.T) {
	v := validator()
	b, _ := Encode(good())
	if _, err := v.DecodeObservation(b); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	bad := map[string]string{
		"not json":   `nope`,
		"unknown":    `{"observation_id":"o","surprise":1}`,
		"trailing":   string(b) + `{}`,
		"wrong type": `{"observation_id": 5}`,
		"truncated":  string(b[:len(b)/2]),
		"empty":      ``,
	}
	for name, body := range bad {
		_, err := v.DecodeObservation([]byte(body))
		if err == nil || domain.CategoryOf(err) != domain.CategoryValidation {
			t.Errorf("%s: err=%v", name, err)
		}
	}
	v.Limits.MaxMessageBytes = 10
	if _, err := v.DecodeObservation(b); domain.CategoryOf(err) != domain.CategoryValidation {
		t.Fatalf("oversize: %v", err)
	}
}

func TestNumbersSurviveRoundTripAsFloat64(t *testing.T) {
	o := good()
	o.Value = int64(42)
	b, _ := Encode(o)
	got, err := validator().DecodeObservation(b)
	if err != nil {
		t.Fatal(err)
	}
	if f, ok := got.Value.(float64); !ok || f != 42 {
		t.Fatalf("value should decode as float64 42, got %T %v", got.Value, got.Value)
	}
}

// The JSON Schema document is documentation, but documentation that drifts is
// worse than none: fail if the struct and the schema disagree on field names.
func TestJSONSchemaDocumentMatchesStruct(t *testing.T) {
	raw, err := os.ReadFile("../../docs/schemas/observation.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	rt := reflect.TypeOf(domain.Observation{})
	fields := map[string]bool{}
	for i := 0; i < rt.NumField(); i++ {
		name := strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]
		fields[name] = true
		if _, ok := doc.Properties[name]; !ok {
			t.Errorf("schema is missing property %q", name)
		}
	}
	for name := range doc.Properties {
		if !fields[name] {
			t.Errorf("schema documents %q, which the struct does not have", name)
		}
	}
	// Everything the validator requires must be documented as required.
	req := map[string]bool{}
	for _, r := range doc.Required {
		req[r] = true
	}
	for _, want := range []string{"observation_id", "correlation_id", "device_id", "source", "metric", "value", "observed_at"} {
		if !req[want] {
			t.Errorf("schema should require %q", want)
		}
	}
}
