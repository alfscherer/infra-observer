// Package state derives current state from observations.
//
// The distinction it enforces is the point of the project:
//
//	observation -> state -> transition -> event -> alert
//
// A single failed sample is an observation. Only a sustained pattern moves the
// state machine, only some transitions produce events, and only opening events
// produce alerts. Firing alerts straight from samples produces alert storms
// from one lost packet; a state machine absorbs noise deliberately.
package state

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// EventSpec describes an event emitted on a transition.
type EventSpec struct {
	Event    string          `yaml:"event"`
	Severity domain.Severity `yaml:"severity"`
	Message  string          `yaml:"message"`
}

// GoodWhen classifies a boolean metric.
type GoodWhen struct {
	Equals *bool `yaml:"equals"`
}

// Threshold classifies a numeric metric with hysteresis: the value must cross
// Breach to become bad, and then cross the (less extreme) Clear to become good
// again, so a value hovering near one line does not flap.
type Threshold struct {
	BreachAbove *float64 `yaml:"breach_above"`
	ClearBelow  *float64 `yaml:"clear_below"`
	BreachBelow *float64 `yaml:"breach_below"`
	ClearAbove  *float64 `yaml:"clear_above"`
}

// Definition declares one tracked state: what is observed, how a sample is
// judged good or bad, and how many samples it takes to change state.
type Definition struct {
	ID          string   `yaml:"id"`
	Description string   `yaml:"description"`
	Metric      string   `yaml:"metric"`
	KeyLabels   []string `yaml:"key_labels"`   // labels identifying the entity; absent labels count as empty
	DeviceTypes []string `yaml:"device_types"` // restrict to these types; empty means all

	GoodWhen  *GoodWhen  `yaml:"good_when"`
	Threshold *Threshold `yaml:"threshold"`

	FailAfter    int           `yaml:"fail_after"`    // consecutive bad samples to reach DOWN
	FailFor      time.Duration `yaml:"fail_for"`      // ...and the streak must have lasted this long (debounce)
	RecoverAfter int           `yaml:"recover_after"` // consecutive good samples to return to UP
	RecoverFor   time.Duration `yaml:"recover_for"`

	// Stale marks the definition as subject to stale-data detection: silence
	// counts as a bad sample.
	Stale bool `yaml:"stale"`

	Alert    EventSpec `yaml:"alert"`    // emitted on entering DOWN
	Recovery EventSpec `yaml:"recovery"` // emitted on returning to UP after DOWN
}

func (d Definition) validate() error {
	var p []string
	if d.ID == "" {
		p = append(p, "id is required")
	}
	if d.Metric == "" {
		p = append(p, "metric is required")
	}
	if (d.GoodWhen == nil) == (d.Threshold == nil) {
		p = append(p, "exactly one of good_when or threshold is required")
	}
	if d.GoodWhen != nil && d.GoodWhen.Equals == nil {
		p = append(p, "good_when.equals is required")
	}
	if t := d.Threshold; t != nil {
		above := t.BreachAbove != nil || t.ClearBelow != nil
		below := t.BreachBelow != nil || t.ClearAbove != nil
		switch {
		case above == below:
			p = append(p, "threshold needs either breach_above+clear_below or breach_below+clear_above")
		case above && (t.BreachAbove == nil || t.ClearBelow == nil):
			p = append(p, "threshold needs both breach_above and clear_below")
		case above && *t.ClearBelow > *t.BreachAbove:
			p = append(p, "clear_below must not exceed breach_above")
		case below && (t.BreachBelow == nil || t.ClearAbove == nil):
			p = append(p, "threshold needs both breach_below and clear_above")
		case below && *t.ClearAbove < *t.BreachBelow:
			p = append(p, "clear_above must not be less than breach_below")
		}
	}
	if d.FailAfter < 1 || d.RecoverAfter < 1 {
		p = append(p, "fail_after and recover_after must be at least 1")
	}
	if d.FailFor < 0 || d.RecoverFor < 0 {
		p = append(p, "fail_for and recover_for must not be negative")
	}
	if d.Alert.Event == "" || !d.Alert.Severity.Valid() {
		p = append(p, "alert.event and a valid alert.severity are required")
	}
	if d.Recovery.Event == "" || !d.Recovery.Severity.Valid() {
		p = append(p, "recovery.event and a valid recovery.severity are required")
	}
	for _, t := range d.DeviceTypes {
		if !domain.DeviceType(t).Valid() {
			p = append(p, fmt.Sprintf("unknown device type %q", t))
		}
	}
	if len(p) > 0 {
		return domain.Errorf(domain.CategoryValidation, "state definition %q: %s", d.ID, strings.Join(p, "; "))
	}
	return nil
}

func (d Definition) appliesTo(t domain.DeviceType) bool {
	if len(d.DeviceTypes) == 0 {
		return true
	}
	for _, x := range d.DeviceTypes {
		if domain.DeviceType(x) == t {
			return true
		}
	}
	return false
}

// Definitions is a validated, indexed set.
type Definitions struct {
	all      []Definition
	byMetric map[string][]Definition
}

// LoadFile reads a definitions file.
func LoadFile(path string) (*Definitions, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read state definitions: %w", err)
	}
	ds, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("state definitions %s: %w", path, err)
	}
	return ds, nil
}

// Parse decodes definitions strictly.
func Parse(raw []byte) (*Definitions, error) {
	var doc struct {
		Definitions []Definition `yaml:"definitions"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	ds := &Definitions{byMetric: map[string][]Definition{}}
	seen := map[string]bool{}
	for _, d := range doc.Definitions {
		if err := d.validate(); err != nil {
			return nil, err
		}
		if seen[d.ID] {
			return nil, domain.Errorf(domain.CategoryValidation, "duplicate state definition %q", d.ID)
		}
		seen[d.ID] = true
		ds.all = append(ds.all, d)
		ds.byMetric[d.Metric] = append(ds.byMetric[d.Metric], d)
	}
	return ds, nil
}

// All returns every definition in file order.
func (ds *Definitions) All() []Definition { return append([]Definition(nil), ds.all...) }

// For returns the definitions that track metric on a device of the given type.
func (ds *Definitions) For(metric string, t domain.DeviceType) []Definition {
	var out []Definition
	for _, d := range ds.byMetric[metric] {
		if d.appliesTo(t) {
			out = append(out, d)
		}
	}
	return out
}

// Get returns a definition by id.
func (ds *Definitions) Get(id string) (Definition, bool) {
	for _, d := range ds.all {
		if d.ID == id {
			return d, true
		}
	}
	return Definition{}, false
}
