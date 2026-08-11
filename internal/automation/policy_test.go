package automation

import (
	"strings"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

func TestSpecExamplePolicyParses(t *testing.T) {
	ps, err := Parse([]byte(`
policies:
  - id: restart-stale-agent
    trigger:
      event: agent.unhealthy
      device_type:
        - server
        - workstation
    conditions:
      duration: 10m
      retries_below: 2
    action:
      type: restart_service
      service: observer-agent
    safety:
      dry_run: true
      cooldown: 30m
      require_tag: automation-enabled
`))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := ps.Get("restart-stale-agent")
	if p.Action.Type != "restart_service" || p.Action.Params["service"] != "observer-agent" || p.Conditions.Duration != 10*time.Minute ||
		p.Conditions.RetriesBelow != 2 || p.Safety.Cooldown != 30*time.Minute || len(p.Trigger.DeviceType) != 2 || !p.dryRun() {
		t.Fatalf("%+v", p)
	}
}

func TestSafeDefaults(t *testing.T) {
	ps, err := Parse([]byte("policies:\n  - {id: p, trigger: {event: e}, action: {type: noop}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := ps.Get("p")
	if !p.dryRun() || p.Safety.RequireTag != DefaultRequireTag || p.Safety.Cooldown != DefaultCooldown {
		t.Fatalf("a policy that says nothing about safety must still be safe: %+v", p.Safety)
	}
}

func TestPolicyValidation(t *testing.T) {
	base := "policies:\n  - id: p\n    trigger: {event: e}\n"
	bad := map[string]string{
		"no event":          "policies:\n  - {id: p, action: {type: noop}}\n",
		"no action":         base,
		"both":              base + "    action: {type: noop}\n    proposal: {script: s, allowed_actions: [noop]}\n",
		"unknown action":    base + "    action: {type: format_disk}\n",
		"missing param":     base + "    action: {type: restart_service}\n",
		"bad param":         base + "    action: {type: restart_service, service: 'x; reboot'}\n",
		"unknown param":     base + "    action: {type: restart_service, service: x, force: y}\n",
		"missing target":    base + "    action: {type: bounce_interface}\n",
		"unexpected target": base + "    action: {type: noop, target: x}\n",
		"script no bound":   base + "    proposal: {script: s}\n",
		"script bad action": base + "    proposal: {script: s, allowed_actions: [format_disk]}\n",
		"script no name":    base + "    proposal: {allowed_actions: [noop]}\n",
		"bad device type":   "policies:\n  - {id: p, trigger: {event: e, device_type: toaster}, action: {type: noop}}\n",
		"bad severity":      "policies:\n  - {id: p, trigger: {event: e, min_severity: loud}, action: {type: noop}}\n",
		"negative cooldown": base + "    action: {type: noop}\n    safety: {cooldown: -1m}\n",
		"unknown key":       base + "    action: {type: noop}\n    colour: red\n",
	}
	for name, doc := range bad {
		if _, err := Parse([]byte(doc)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	dup := "policies:\n  - {id: p, trigger: {event: e}, action: {type: noop}}\n"
	if _, err := Parse([]byte(dup + dup[len("policies:\n"):])); err == nil {
		t.Error("duplicate policy ids must be rejected")
	}
	// templated values are validated when the event is known, not at load
	if _, err := Parse([]byte(base + "    action: {type: bounce_interface, target: '{interface}'}\n")); err != nil {
		t.Errorf("templated target should load: %v", err)
	}
}

func TestMaintenanceWindows(t *testing.T) {
	mon := func(h, m int) time.Time { return time.Date(2026, 8, 3, h, m, 0, 0, time.UTC) } // Monday
	dev := domain.Device{ID: "d", Site: "lab", Tags: []string{"patch"}}
	rec := func(days []string, start, end string) Window {
		w := Window{ID: "w"}
		w.Recurring = &struct {
			Days     StringList `yaml:"days"`
			Start    string     `yaml:"start"`
			End      string     `yaml:"end"`
			Timezone string     `yaml:"timezone"`
		}{Days: days, Start: start, End: end}
		return w
	}
	cases := []struct {
		name string
		w    Window
		now  time.Time
		want bool
	}{
		{"inside", rec([]string{"mon"}, "11:00", "13:00"), mon(12, 0), true},
		{"start inclusive", rec([]string{"mon"}, "11:00", "13:00"), mon(11, 0), true},
		{"end exclusive", rec([]string{"mon"}, "11:00", "13:00"), mon(13, 0), false},
		{"wrong day", rec([]string{"tue"}, "11:00", "13:00"), mon(12, 0), false},
		{"any day", rec(nil, "11:00", "13:00"), mon(12, 0), true},
		{"crosses midnight, evening part", rec([]string{"mon"}, "22:00", "02:00"), mon(23, 0), true},
		{"crosses midnight, morning part belongs to the previous day", rec([]string{"sun"}, "22:00", "02:00"), mon(1, 0), true},
		{"crosses midnight, wrong morning", rec([]string{"mon"}, "22:00", "02:00"), mon(1, 0), false},
		{"absolute inside", Window{ID: "a", From: mon(0, 0), Until: mon(23, 0)}, mon(12, 0), true},
		{"absolute after", Window{ID: "a", From: mon(0, 0), Until: mon(10, 0)}, mon(12, 0), false},
	}
	for _, c := range cases {
		if got := c.w.Active(c.now, dev); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
	scoped := rec([]string{"mon"}, "11:00", "13:00")
	scoped.Match.Tags = StringList{"other"}
	if scoped.Active(mon(12, 0), dev) {
		t.Error("a window scoped to other tags must not cover this device")
	}
	scoped.Match.Tags = StringList{"patch"}
	if !scoped.Active(mon(12, 0), dev) {
		t.Error("tag scope")
	}
	if _, err := Parse([]byte("maintenance_windows:\n  - {id: w}\n")); err == nil {
		t.Error("a window needs a schedule")
	}
	if _, err := Parse([]byte("maintenance_windows:\n  - {id: w, recurring: {start: '25:00', end: '02:00'}}\n")); err == nil {
		t.Error("bad time")
	}
}

func TestActionValidation(t *testing.T) {
	sw := domain.Device{ID: "sw", DeviceType: domain.DeviceSwitch}
	srv := domain.Device{ID: "srv", DeviceType: domain.DeviceServer}
	ok := []struct {
		p   Proposal
		dev domain.Device
	}{
		{Proposal{Action: "bounce_interface", Target: "Gi0/1"}, sw},
		{Proposal{Action: "set_interface_description", Target: "Gi0/1", Params: map[string]string{"description": "uplink to core"}}, sw},
		{Proposal{Action: "restart_service", Params: map[string]string{"service": "observer-agent"}}, srv},
		{Proposal{Action: "noop"}, srv},
	}
	for _, c := range ok {
		if err := Validate(c.p, c.dev); err != nil {
			t.Errorf("%+v: %v", c.p, err)
		}
	}
	bad := []Proposal{
		{Action: "restart_service", Params: map[string]string{"service": "a; rm -rf /"}},
		{Action: "restart_service", Params: map[string]string{"service": "$(id)"}},
		{Action: "restart_service", Params: map[string]string{"service": "`id`"}},
		{Action: "restart_service", Params: map[string]string{"service": "a b"}},
		{Action: "restart_service", Params: map[string]string{"service": "../../etc/passwd"}},
		{Action: "restart_service", Params: map[string]string{"service": strings.Repeat("a", 200)}},
		{Action: "restart_service", Params: map[string]string{"service": "ok", "extra": "x"}},
		{Action: "restart_service"},
		{Action: "restart_service", Target: "nope", Params: map[string]string{"service": "ok"}},
		{Action: "bounce_interface", Target: "Gi0/1\nreboot"},
		{Action: "bounce_interface", Target: ""},
		{Action: "shell", Params: map[string]string{"cmd": "id"}},
		{Action: ""},
	}
	for _, p := range bad {
		dev := srv
		if p.Action == "bounce_interface" {
			dev = sw
		}
		err := Validate(p, dev)
		if err == nil || domain.CategoryOf(err) != domain.CategoryValidation {
			t.Errorf("%+v must be rejected as a validation error, got %v", p, err)
		}
	}
	if err := Validate(Proposal{Action: "bounce_interface", Target: "Gi0/1"}, srv); err == nil {
		t.Error("a switch action must not apply to a server")
	}
	if err := Validate(Proposal{Action: "restart_service", Params: map[string]string{"service": "x"}}, sw); err == nil {
		t.Error("a host action must not apply to a switch")
	}
}

func TestNoCatalogActionIsAShell(t *testing.T) {
	for name, spec := range Catalog {
		for _, banned := range []string{"exec", "shell", "command", "script_body", "cmd"} {
			if strings.Contains(name, banned) {
				t.Errorf("action %q looks like arbitrary execution", name)
			}
			for _, ps := range spec.Params {
				if ps.Name == banned {
					t.Errorf("action %s exposes a %q parameter", name, banned)
				}
			}
		}
		for _, ps := range spec.Params {
			for _, hostile := range []string{"a;b", "a|b", "a&b", "a`b`", "$(b)", "a\nb", "a>b", "a<b"} {
				if ps.Pattern.MatchString(hostile) {
					t.Errorf("parameter %s of %s accepts shell metacharacters: %q", ps.Name, name, hostile)
				}
			}
		}
	}
}

func TestShippedPoliciesLoadAndAreDryRunByDefault(t *testing.T) {
	ps, err := LoadFile("../../configs/automation.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(ps.All()) < 4 {
		t.Fatalf("policies: %d", len(ps.All()))
	}
	for _, p := range ps.All() {
		if !p.dryRun() {
			t.Errorf("shipped policy %s must default to dry-run", p.ID)
		}
		if p.Safety.RequireTag == "" || p.Safety.Cooldown == 0 {
			t.Errorf("shipped policy %s lacks guard rails", p.ID)
		}
	}
	// the spec's example: trigger + tag gate + duration
	p, _ := ps.Get("restart-stale-agent")
	if p.Conditions.Duration != 10*time.Minute || p.Conditions.RetriesBelow != 2 || p.Safety.Cooldown != 30*time.Minute {
		t.Fatalf("%+v", p)
	}
}
