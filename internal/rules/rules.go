// Package rules evaluates configurable monitoring rules over time windows.
//
// State definitions (package state) answer "is this entity healthy right now,
// judged by consecutive samples?". Rules answer questions about a series over
// a window: "did this interface change status five times in ten minutes?",
// "has average CPU stayed above 90% for five minutes?". They are data, not
// code: a match, one condition, a severity. There is deliberately no
// expression language; a rule that needs one is a script's job.
package rules

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/state"
)

// StringList accepts either a scalar or a list in YAML.
type StringList []string

func (l *StringList) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		*l = StringList{n.Value}
		return nil
	case yaml.SequenceNode:
		var out []string
		if err := n.Decode(&out); err != nil {
			return err
		}
		*l = out
		return nil
	}
	return fmt.Errorf("line %d: expected a string or a list of strings", n.Line)
}

// Match selects which observations a rule looks at.
type Match struct {
	DeviceType StringList `yaml:"device_type"`
	Metric     string     `yaml:"metric"`
	Site       string     `yaml:"site"`
	Tags       StringList `yaml:"tags"` // device must carry all of them
}

// Condition is exactly one of: a transition count, or a windowed aggregate
// compared against a limit.
type Condition struct {
	Transitions int           `yaml:"transitions"` // status changes of a boolean series...
	Within      time.Duration `yaml:"within"`      // ...inside this window

	AverageOver time.Duration `yaml:"average_over"`
	MaxOver     time.Duration `yaml:"max_over"`
	MinOver     time.Duration `yaml:"min_over"`
	GreaterThan *float64      `yaml:"greater_than"`
	LessThan    *float64      `yaml:"less_than"`
	MinSamples  int           `yaml:"min_samples"` // an aggregate over too few samples is "no data", never true
}

// Rule is one monitoring rule.
type Rule struct {
	ID          string          `yaml:"id"`
	Description string          `yaml:"description"`
	Match       Match           `yaml:"match"`
	Condition   Condition       `yaml:"condition"`
	Severity    domain.Severity `yaml:"severity"`
	Event       string          `yaml:"event"`       // default "rule.triggered"
	ClearEvent  string          `yaml:"clear_event"` // default "rule.cleared"
	Message     string          `yaml:"message"`
}

// Window is how far back the rule needs samples.
func (r Rule) Window() time.Duration {
	c := r.Condition
	switch {
	case c.Transitions > 0:
		return c.Within
	case c.AverageOver > 0:
		return c.AverageOver
	case c.MaxOver > 0:
		return c.MaxOver
	}
	return c.MinOver
}

func (r Rule) validate() error {
	var p []string
	if r.ID == "" {
		p = append(p, "id is required")
	}
	if r.Match.Metric == "" {
		p = append(p, "match.metric is required")
	}
	for _, t := range r.Match.DeviceType {
		if !domain.DeviceType(t).Valid() {
			p = append(p, fmt.Sprintf("unknown device type %q", t))
		}
	}
	if !r.Severity.Valid() {
		p = append(p, "severity must be info, warning or critical")
	}
	c := r.Condition
	kinds := 0
	if c.Transitions > 0 || c.Within > 0 {
		kinds++
		if c.Transitions < 1 || c.Within <= 0 {
			p = append(p, "transitions needs both a count >= 1 and a within window")
		}
	}
	aggs := 0
	for _, d := range []time.Duration{c.AverageOver, c.MaxOver, c.MinOver} {
		if d != 0 {
			aggs++
			if d < 0 {
				p = append(p, "aggregate windows must be positive")
			}
		}
	}
	if aggs > 1 {
		p = append(p, "use only one of average_over, max_over, min_over")
	}
	if aggs == 1 {
		kinds++
		if (c.GreaterThan == nil) == (c.LessThan == nil) {
			p = append(p, "an aggregate needs exactly one of greater_than or less_than")
		}
	} else if c.GreaterThan != nil || c.LessThan != nil {
		p = append(p, "greater_than/less_than need average_over, max_over or min_over")
	}
	if kinds != 1 {
		p = append(p, "condition must be exactly one of transitions+within or an aggregate")
	}
	if c.MinSamples < 0 {
		p = append(p, "min_samples must not be negative")
	}
	if len(p) > 0 {
		return domain.Errorf(domain.CategoryValidation, "rule %q: %s", r.ID, strings.Join(p, "; "))
	}
	return nil
}

func (r Rule) eventName() string {
	if r.Event != "" {
		return r.Event
	}
	return "rule.triggered"
}

func (r Rule) clearEventName() string {
	if r.ClearEvent != "" {
		return r.ClearEvent
	}
	return "rule.cleared"
}

// Rules is a validated, indexed set.
type Rules struct {
	all      []Rule
	byMetric map[string][]Rule
}

// LoadFile reads a rules file.
func LoadFile(path string) (*Rules, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read rules: %w", err)
	}
	rs, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("rules %s: %w", path, err)
	}
	return rs, nil
}

// Parse decodes rules strictly.
func Parse(raw []byte) (*Rules, error) {
	var doc struct {
		Rules []Rule `yaml:"rules"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	rs := &Rules{byMetric: map[string][]Rule{}}
	seen := map[string]bool{}
	for _, r := range doc.Rules {
		if err := r.validate(); err != nil {
			return nil, err
		}
		if seen[r.ID] {
			return nil, domain.Errorf(domain.CategoryValidation, "duplicate rule id %q", r.ID)
		}
		seen[r.ID] = true
		rs.all = append(rs.all, r)
		rs.byMetric[r.Match.Metric] = append(rs.byMetric[r.Match.Metric], r)
	}
	return rs, nil
}

// All returns every rule.
func (rs *Rules) All() []Rule {
	if rs == nil {
		return nil
	}
	return append([]Rule(nil), rs.all...)
}

// For returns the rules that apply to a metric observed on dev.
func (rs *Rules) For(metric string, dev domain.Device) []Rule {
	if rs == nil {
		return nil
	}
	var out []Rule
	for _, r := range rs.byMetric[metric] {
		if len(r.Match.DeviceType) > 0 && !contains(r.Match.DeviceType, string(dev.DeviceType)) {
			continue
		}
		if r.Match.Site != "" && r.Match.Site != dev.Site {
			continue
		}
		ok := true
		for _, tag := range r.Match.Tags {
			if !dev.HasTag(tag) {
				ok = false
			}
		}
		if ok {
			out = append(out, r)
		}
	}
	return out
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

// KeyFor identifies the rule's tracked entity: one activation per rule,
// device and full label set (so every interface is judged separately).
func KeyFor(r Rule, deviceID string, labels map[string]string) (string, map[string]string) {
	return "rule:" + r.ID + "/" + deviceID + "/" + domain.LabelsKey(labels), labels
}

// Evaluate applies a rule at the time of observation o, given the series
// samples up to and including o. It is pure: no I/O, no clock.
func Evaluate(r Rule, prev *domain.StateRecord, samples []domain.Sample, o domain.Observation) state.Update {
	key, labels := KeyFor(r, o.DeviceID, o.Labels)
	if prev != nil {
		if prev.LastObservationID == o.ObservationID {
			return state.Update{Record: *prev, Ignored: state.Duplicate}
		}
		if !o.ObservedAt.After(prev.LastObservedAt) {
			return state.Update{Record: *prev, Ignored: state.Stale}
		}
	}
	rec := domain.StateRecord{Key: key, DeviceID: o.DeviceID, DefinitionID: "rule:" + r.ID, Labels: cloneLabels(labels), State: domain.StateUp}
	if prev != nil {
		rec = *prev
	}
	active := prev != nil && prev.State == domain.StateDown

	holds, measured := condition(r.Condition, samples)
	rec.LastObservedAt, rec.LastObservationID, rec.LastSeenAt = o.ObservedAt, o.ObservationID, o.ObservedAt
	rec.LastValue = strconv.FormatFloat(measured, 'g', 6, 64)

	up := state.Update{Record: rec}
	var spec struct {
		name     string
		severity domain.Severity
		resolves bool
	}
	switch {
	case !active && holds:
		rec.State, rec.LastTransitionAt = domain.StateDown, o.ObservedAt
		spec.name, spec.severity = r.eventName(), r.Severity
	case active && !holds:
		rec.State, rec.LastTransitionAt = domain.StateUp, o.ObservedAt
		spec.name, spec.severity, spec.resolves = r.clearEventName(), domain.SeverityInfo, true
	default:
		if prev == nil {
			rec.LastTransitionAt = o.ObservedAt
		}
		up.Record = rec
		return up
	}
	up.Record = rec
	up.Transitions = []domain.Transition{{
		TransitionID: domain.StableID("tr", key, o.ObservationID), Key: key, DeviceID: o.DeviceID, DefinitionID: rec.DefinitionID,
		Labels: rec.Labels, From: prevState(prev), To: rec.State, At: o.ObservedAt, ObservationID: o.ObservationID, CorrelationID: o.CorrelationID,
	}}
	evLabels := cloneLabels(o.Labels)
	if evLabels == nil {
		evLabels = map[string]string{}
	}
	evLabels["rule_id"] = r.ID
	up.Events = []domain.Event{{
		EventID:       domain.StableID("evt", o.DeviceID, r.ID, domain.LabelsKey(evLabels), spec.name, o.ObservationID),
		CorrelationID: o.CorrelationID, DeviceID: o.DeviceID, Type: spec.name, Severity: spec.severity,
		Message: render(r, spec.resolves, o, measured), Labels: evLabels, OccurredAt: o.ObservedAt,
		ObservationID: o.ObservationID, Source: "rule:" + r.ID, AlertKey: key, Resolves: spec.resolves,
	}}
	return up
}

func prevState(p *domain.StateRecord) domain.HealthState {
	if p == nil {
		return domain.StateUp
	}
	return p.State
}

func cloneLabels(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	c := make(map[string]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// condition reports whether the rule's condition holds over samples, along
// with the measured number (flip count or aggregate) for messages.
func condition(c Condition, samples []domain.Sample) (holds bool, measured float64) {
	if c.Transitions > 0 {
		flips := 0
		for i := 1; i < len(samples); i++ {
			if truthy(samples[i]) != truthy(samples[i-1]) {
				flips++
			}
		}
		return flips >= c.Transitions, float64(flips)
	}
	minSamples := c.MinSamples
	if minSamples == 0 {
		minSamples = 3
	}
	if len(samples) < minSamples {
		return false, 0 // not enough data is not a breach
	}
	switch {
	case c.AverageOver > 0:
		var sum float64
		for _, s := range samples {
			sum += s.Num
		}
		measured = sum / float64(len(samples))
	case c.MaxOver > 0:
		measured = samples[0].Num
		for _, s := range samples {
			measured = max(measured, s.Num)
		}
	default:
		measured = samples[0].Num
		for _, s := range samples {
			measured = min(measured, s.Num)
		}
	}
	if c.GreaterThan != nil {
		return measured > *c.GreaterThan, measured
	}
	return measured < *c.LessThan, measured
}

func truthy(s domain.Sample) bool {
	if s.Bool != nil {
		return *s.Bool
	}
	return s.Num != 0
}

func render(r Rule, clear bool, o domain.Observation, measured float64) string {
	tmpl := r.Message
	if tmpl == "" {
		if clear {
			return fmt.Sprintf("rule %s cleared on %s", r.ID, o.DeviceID)
		}
		return fmt.Sprintf("rule %s triggered on %s", r.ID, o.DeviceID)
	}
	pairs := []string{"{device}", o.DeviceID, "{value}", strconv.FormatFloat(measured, 'g', 6, 64), "{rule}", r.ID}
	for k, v := range o.Labels {
		pairs = append(pairs, "{"+k+"}", v)
	}
	return strings.NewReplacer(pairs...).Replace(tmpl)
}
