package automation

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// StringList accepts a scalar or a list in YAML.
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

// Trigger selects the events a policy reacts to.
type Trigger struct {
	Event       string          `yaml:"event"`
	DeviceType  StringList      `yaml:"device_type"`
	Site        string          `yaml:"site"`
	Tags        StringList      `yaml:"tags"`
	MinSeverity domain.Severity `yaml:"min_severity"`
}

// Conditions are checked when the request is due, not when it is created.
type Conditions struct {
	// Duration delays the action: the alert must still be firing this long
	// after it opened. Most incidents heal by themselves; acting on the first
	// sample of a problem is how automation causes outages.
	Duration time.Duration `yaml:"duration"`
	// RetriesBelow caps executed attempts per policy per alert (0 = unlimited).
	RetriesBelow int `yaml:"retries_below"`
}

// ActionRef is a static action configured on a policy. YAML shape:
//
//	action: {type: restart_service, service: observer-agent}
//
// `type` and `target` are reserved; every other key is an action parameter.
type ActionRef struct {
	Type   string
	Target string
	Params map[string]string
}

func (a *ActionRef) UnmarshalYAML(n *yaml.Node) error {
	var raw map[string]string
	if err := n.Decode(&raw); err != nil {
		return err
	}
	a.Type, a.Target, a.Params = raw["type"], raw["target"], map[string]string{}
	for k, v := range raw {
		if k != "type" && k != "target" {
			a.Params[k] = v
		}
	}
	return nil
}

// ProposalRef delegates the choice of action to a JavaScript extension. The
// script only proposes; AllowedActions bounds what the policy will accept.
type ProposalRef struct {
	Script         string     `yaml:"script"`
	AllowedActions StringList `yaml:"allowed_actions"`
}

// Safety holds the guard rails. Every field errs on the side of doing less.
type Safety struct {
	DryRun          *bool         `yaml:"dry_run"`          // default true
	Cooldown        time.Duration `yaml:"cooldown"`         // per policy, device and target
	RequireTag      string        `yaml:"require_tag"`      // device must carry this tag
	RequireApproval bool          `yaml:"require_approval"` // a human must approve
	ApprovalTimeout time.Duration `yaml:"approval_timeout"` // unapproved requests expire
}

// Policy connects an event to a controlled action.
type Policy struct {
	ID          string       `yaml:"id"`
	Description string       `yaml:"description"`
	Trigger     Trigger      `yaml:"trigger"`
	Conditions  Conditions   `yaml:"conditions"`
	Action      *ActionRef   `yaml:"action"`
	Proposal    *ProposalRef `yaml:"proposal"`
	Safety      Safety       `yaml:"safety"`
}

// DryRunByDefault reports whether the policy runs dry unless explicitly told otherwise.
func (p Policy) dryRun() bool { return p.Safety.DryRun == nil || *p.Safety.DryRun }

// Window is a maintenance window during which automation is suppressed for
// matching devices: people are working on them.
type Window struct {
	ID    string `yaml:"id"`
	Match struct {
		Devices StringList `yaml:"devices"`
		Site    string     `yaml:"site"`
		Tags    StringList `yaml:"tags"`
	} `yaml:"match"`
	Recurring *struct {
		Days     StringList `yaml:"days"`     // mon..sun
		Start    string     `yaml:"start"`    // HH:MM
		End      string     `yaml:"end"`      // HH:MM; may be earlier than start to cross midnight
		Timezone string     `yaml:"timezone"` // default UTC
	} `yaml:"recurring"`
	From  time.Time `yaml:"from"`
	Until time.Time `yaml:"until"`
}

var dayNames = map[string]time.Weekday{"sun": time.Sunday, "mon": time.Monday, "tue": time.Tuesday, "wed": time.Wednesday, "thu": time.Thursday, "fri": time.Friday, "sat": time.Saturday}

func parseHM(s string) (int, error) {
	t, err := time.Parse("15:04", s)
	if err != nil {
		return 0, fmt.Errorf("%q is not HH:MM", s)
	}
	return t.Hour()*60 + t.Minute(), nil
}

func (w Window) covers(dev domain.Device) bool {
	m := w.Match
	if len(m.Devices) == 0 && m.Site == "" && len(m.Tags) == 0 {
		return true
	}
	for _, id := range m.Devices {
		if id == dev.ID {
			return true
		}
	}
	if m.Site != "" && m.Site == dev.Site {
		return true
	}
	for _, t := range m.Tags {
		if dev.HasTag(t) {
			return true
		}
	}
	return false
}

// Active reports whether now falls inside the window for dev.
func (w Window) Active(now time.Time, dev domain.Device) bool {
	if !w.covers(dev) {
		return false
	}
	if !w.From.IsZero() || !w.Until.IsZero() {
		if !w.From.IsZero() && now.Before(w.From) {
			return false
		}
		if !w.Until.IsZero() && !now.Before(w.Until) {
			return false
		}
		return true
	}
	r := w.Recurring
	if r == nil {
		return false
	}
	loc := time.UTC
	if r.Timezone != "" {
		if l, err := time.LoadLocation(r.Timezone); err == nil {
			loc = l
		}
	}
	local := now.In(loc)
	start, _ := parseHM(r.Start)
	end, _ := parseHM(r.End)
	cur := local.Hour()*60 + local.Minute()
	dayOK := func(d time.Weekday) bool {
		if len(r.Days) == 0 {
			return true
		}
		for _, n := range r.Days {
			if dayNames[strings.ToLower(n)] == d {
				return true
			}
		}
		return false
	}
	if start <= end {
		return dayOK(local.Weekday()) && cur >= start && cur < end
	}
	// crosses midnight: the window belongs to the day it started on
	if cur >= start {
		return dayOK(local.Weekday())
	}
	if cur < end {
		return dayOK((local.Weekday() + 6) % 7)
	}
	return false
}

// Policies is a validated policy file.
type Policies struct {
	Allowlist []string
	Windows   []Window
	policies  []Policy
	byID      map[string]Policy
}

type file struct {
	Allowlist struct {
		Devices StringList `yaml:"devices"`
	} `yaml:"allowlist"`
	MaintenanceWindows []Window `yaml:"maintenance_windows"`
	Policies           []Policy `yaml:"policies"`
}

// Default applied when a policy does not set them: a policy never runs
// without a tag gate, and repeated actions are rate limited.
const (
	DefaultRequireTag      = "automation-enabled"
	DefaultCooldown        = 15 * time.Minute
	DefaultApprovalTimeout = time.Hour
)

// LoadFile reads a policy file.
func LoadFile(path string) (*Policies, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read automation policies: %w", err)
	}
	ps, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("automation policies %s: %w", path, err)
	}
	return ps, nil
}

// Parse decodes and validates policies.
func Parse(raw []byte) (*Policies, error) {
	var f file
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, err
	}
	ps := &Policies{Allowlist: f.Allowlist.Devices, Windows: f.MaintenanceWindows, byID: map[string]Policy{}}
	for _, w := range f.MaintenanceWindows {
		if err := validateWindow(w); err != nil {
			return nil, err
		}
	}
	for _, pol := range f.Policies {
		if pol.Safety.RequireTag == "" {
			pol.Safety.RequireTag = DefaultRequireTag
		}
		if pol.Safety.Cooldown == 0 {
			pol.Safety.Cooldown = DefaultCooldown
		}
		if pol.Safety.RequireApproval && pol.Safety.ApprovalTimeout == 0 {
			pol.Safety.ApprovalTimeout = DefaultApprovalTimeout
		}
		if err := validatePolicy(pol); err != nil {
			return nil, err
		}
		if _, dup := ps.byID[pol.ID]; dup {
			return nil, domain.Errorf(domain.CategoryValidation, "duplicate policy id %q", pol.ID)
		}
		ps.byID[pol.ID] = pol
		ps.policies = append(ps.policies, pol)
	}
	return ps, nil
}

func validateWindow(w Window) error {
	if w.ID == "" {
		return domain.Errorf(domain.CategoryValidation, "maintenance window needs an id")
	}
	hasAbs := !w.From.IsZero() || !w.Until.IsZero()
	if hasAbs == (w.Recurring != nil) {
		return domain.Errorf(domain.CategoryValidation, "maintenance window %q needs exactly one of recurring or from/until", w.ID)
	}
	if w.Recurring != nil {
		if _, err := parseHM(w.Recurring.Start); err != nil {
			return domain.Errorf(domain.CategoryValidation, "window %q start: %v", w.ID, err)
		}
		if _, err := parseHM(w.Recurring.End); err != nil {
			return domain.Errorf(domain.CategoryValidation, "window %q end: %v", w.ID, err)
		}
		for _, d := range w.Recurring.Days {
			if _, ok := dayNames[strings.ToLower(d)]; !ok {
				return domain.Errorf(domain.CategoryValidation, "window %q: unknown day %q", w.ID, d)
			}
		}
		if w.Recurring.Timezone != "" {
			if _, err := time.LoadLocation(w.Recurring.Timezone); err != nil {
				return domain.Errorf(domain.CategoryValidation, "window %q: unknown timezone %q", w.ID, w.Recurring.Timezone)
			}
		}
	}
	return nil
}

func validatePolicy(p Policy) error {
	var problems []string
	if p.ID == "" {
		problems = append(problems, "id is required")
	}
	if p.Trigger.Event == "" {
		problems = append(problems, "trigger.event is required")
	}
	for _, t := range p.Trigger.DeviceType {
		if !domain.DeviceType(t).Valid() {
			problems = append(problems, fmt.Sprintf("unknown device type %q", t))
		}
	}
	if p.Trigger.MinSeverity != "" && !p.Trigger.MinSeverity.Valid() {
		problems = append(problems, "trigger.min_severity is invalid")
	}
	if (p.Action == nil) == (p.Proposal == nil) {
		problems = append(problems, "exactly one of action or proposal is required")
	}
	if p.Action != nil {
		spec, ok := Catalog[p.Action.Type]
		if !ok {
			problems = append(problems, fmt.Sprintf("unknown action type %q (catalog: %s)", p.Action.Type, strings.Join(CatalogNames(), ", ")))
		} else if !strings.Contains(p.Action.Target, "{") && !hasTemplate(p.Action.Params) {
			// fully static: validate it now so a typo fails at startup, not at 3am
			if err := validateShape(spec, Proposal{Action: p.Action.Type, Target: p.Action.Target, Params: p.Action.Params}); err != nil {
				problems = append(problems, err.Error())
			}
		}
	}
	if p.Proposal != nil {
		if p.Proposal.Script == "" {
			problems = append(problems, "proposal.script is required")
		}
		if len(p.Proposal.AllowedActions) == 0 {
			problems = append(problems, "proposal.allowed_actions is required: a policy must bound what a script may propose")
		}
		for _, a := range p.Proposal.AllowedActions {
			if _, ok := Catalog[a]; !ok {
				problems = append(problems, fmt.Sprintf("proposal.allowed_actions: unknown action %q", a))
			}
		}
	}
	if p.Conditions.Duration < 0 || p.Conditions.RetriesBelow < 0 || p.Safety.Cooldown < 0 || p.Safety.ApprovalTimeout < 0 {
		problems = append(problems, "durations and counts must not be negative")
	}
	if len(problems) > 0 {
		return domain.Errorf(domain.CategoryValidation, "policy %q: %s", p.ID, strings.Join(problems, "; "))
	}
	return nil
}

func hasTemplate(m map[string]string) bool {
	for _, v := range m {
		if strings.Contains(v, "{") {
			return true
		}
	}
	return false
}

// All returns the policies in file order.
func (ps *Policies) All() []Policy { return append([]Policy(nil), ps.policies...) }

// Get returns a policy by id.
func (ps *Policies) Get(id string) (Policy, bool) {
	p, ok := ps.byID[id]
	return p, ok
}

// Allowlisted reports whether a device may be the target of live mutation.
func (ps *Policies) Allowlisted(deviceID string) bool {
	for _, d := range ps.Allowlist {
		if d == deviceID {
			return true
		}
	}
	return false
}

// InMaintenance returns the id of an active window covering dev, if any.
func (ps *Policies) InMaintenance(now time.Time, dev domain.Device) (string, bool) {
	for _, w := range ps.Windows {
		if w.Active(now, dev) {
			return w.ID, true
		}
	}
	return "", false
}

// Match returns the policies triggered by ev on dev.
func (ps *Policies) Match(ev domain.Event, dev domain.Device) []Policy {
	var out []Policy
	for _, p := range ps.policies {
		t := p.Trigger
		if t.Event != ev.Type {
			continue
		}
		if len(t.DeviceType) > 0 && !containsStr(t.DeviceType, string(dev.DeviceType)) {
			continue
		}
		if t.Site != "" && t.Site != dev.Site {
			continue
		}
		if t.MinSeverity != "" && ev.Severity.Rank() < t.MinSeverity.Rank() {
			continue
		}
		ok := true
		for _, tag := range t.Tags {
			if !dev.HasTag(tag) {
				ok = false
			}
		}
		if ok {
			out = append(out, p)
		}
	}
	return out
}

func containsStr(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}
