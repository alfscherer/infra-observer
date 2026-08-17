// Package automation is the controlled-response subsystem.
//
// It is deliberately separate from monitoring. Monitoring answers "what is
// true?"; automation answers "may we act on it, and how?". The path between
// them is fixed:
//
//	event -> policy match -> proposal -> validation -> gates -> adapter -> audited result
//
// Nothing here executes because an alert fired. Nothing here executes a
// command line: actions come from a closed catalog with typed parameters, and
// adapters implement each one against a device.
package automation

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// ParamSpec describes one allowed parameter of an action.
type ParamSpec struct {
	Name     string
	Required bool
	Pattern  *regexp.Regexp
}

// ActionSpec describes one action in the catalog. The catalog is the complete
// list of things automation can do; a policy, or a script's proposal, can
// only select from it.
type ActionSpec struct {
	Name        string
	Description string
	DeviceTypes []domain.DeviceType // empty means any
	Capability  string              // capability the device must advertise; empty means none
	NeedsTarget bool
	Target      *regexp.Regexp
	Params      []ParamSpec
	// Disruptive actions change something. They are the ones an allowlist
	// protects when running live; read-only actions are always safe to run.
	Disruptive bool
}

var (
	reIface   = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9/._-]{0,31}$`)
	reName    = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,47}$`)
	reText    = regexp.MustCompile(`^[A-Za-z0-9 .,:/@()_'-]{0,120}$`)
	reVLAN    = regexp.MustCompile(`^[0-9]{1,4}$`)
	reEnpoint = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,47}$`)
)

func p(name string, required bool, re *regexp.Regexp) ParamSpec {
	return ParamSpec{Name: name, Required: required, Pattern: re}
}

var netTypes = []domain.DeviceType{domain.DeviceSwitch, domain.DeviceRouter}
var hostTypes = []domain.DeviceType{domain.DeviceServer, domain.DeviceWorkstation}

// Catalog is the closed set of automation actions.
var Catalog = func() map[string]ActionSpec {
	specs := []ActionSpec{
		// switch
		{Name: "disable_interface", Description: "administratively disable an interface", DeviceTypes: netTypes, Capability: "interface-admin", NeedsTarget: true, Target: reIface, Disruptive: true},
		{Name: "enable_interface", Description: "administratively enable an interface", DeviceTypes: netTypes, Capability: "interface-admin", NeedsTarget: true, Target: reIface, Disruptive: true},
		{Name: "bounce_interface", Description: "disable then enable an interface", DeviceTypes: netTypes, Capability: "interface-admin", NeedsTarget: true, Target: reIface, Disruptive: true},
		{Name: "set_interface_description", Description: "update an interface description", DeviceTypes: netTypes, Capability: "interface-admin", NeedsTarget: true, Target: reIface, Disruptive: true,
			Params: []ParamSpec{p("description", true, reText)}},
		{Name: "query_interface_state", Description: "read an interface's operational and administrative state over SNMP", DeviceTypes: append(append([]domain.DeviceType{}, netTypes...), domain.DeviceAccessPoint), NeedsTarget: true, Target: reIface},
		{Name: "query_vlan_state", Description: "read VLAN state", DeviceTypes: netTypes, Params: []ParamSpec{p("vlan", false, reVLAN)}},
		// access point
		{Name: "restart_ap_service", Description: "restart a service on an access point", DeviceTypes: []domain.DeviceType{domain.DeviceAccessPoint}, Capability: "service-restart", Disruptive: true,
			Params: []ParamSpec{p("service", true, reName)}},
		{Name: "query_ap_clients", Description: "read connected client state", DeviceTypes: []domain.DeviceType{domain.DeviceAccessPoint}},
		{Name: "update_ap_config", Description: "change an access point setting", DeviceTypes: []domain.DeviceType{domain.DeviceAccessPoint}, Capability: "config-update", Disruptive: true,
			Params: []ParamSpec{p("key", true, reName), p("value", true, reText)}},
		// server / workstation
		{Name: "restart_service", Description: "restart a service through the management agent", DeviceTypes: hostTypes, Capability: "service-restart", Disruptive: true,
			Params: []ParamSpec{p("service", true, reName)}},
		{Name: "collect_diagnostics", Description: "gather diagnostic information", DeviceTypes: hostTypes, Capability: "diagnostics"},
		{Name: "run_maintenance_task", Description: "run a predefined maintenance task", DeviceTypes: hostTypes, Capability: "diagnostics", Disruptive: true,
			Params: []ParamSpec{p("task", true, reName)}},
		// generic
		{Name: "send_webhook", Description: "post a notification to a configured endpoint", Params: []ParamSpec{p("endpoint", true, reEnpoint), p("message", false, reText)}},
		{Name: "create_event_record", Description: "record an event in the platform", Params: []ParamSpec{p("message", true, reText), p("severity", false, regexp.MustCompile(`^(info|warning|critical)$`))}},
		{Name: "invoke_extension", Description: "run an approved integration script", Params: []ParamSpec{p("script", true, reName)}},
		{Name: "noop", Description: "do nothing; exercises the policy path"},
	}
	m := make(map[string]ActionSpec, len(specs))
	for _, s := range specs {
		m[s.Name] = s
	}
	return m
}()

// CatalogNames lists action names, sorted.
func CatalogNames() []string {
	names := make([]string, 0, len(Catalog))
	for n := range Catalog {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Proposal is a request to perform one catalog action. Policies produce them
// from static configuration; scripts produce them from JavaScript. Either way
// it is only a proposal until it has passed Validate and the policy gates.
type Proposal struct {
	Action string            `json:"action"`
	Target string            `json:"target,omitempty"`
	Params map[string]string `json:"params,omitempty"`
	Reason string            `json:"reason,omitempty"`
}

// Validate checks a proposal against the catalog and a device. It is the
// same check for every proposer.
func Validate(pr Proposal, dev domain.Device) error {
	spec, ok := Catalog[pr.Action]
	if !ok {
		return domain.Errorf(domain.CategoryValidation, "unknown action %q", pr.Action)
	}
	if err := validateShape(spec, pr); err != nil {
		return err
	}
	if len(spec.DeviceTypes) > 0 {
		match := false
		for _, t := range spec.DeviceTypes {
			if t == dev.DeviceType {
				match = true
			}
		}
		if !match {
			return domain.Errorf(domain.CategoryValidation, "action %s does not apply to %s devices", pr.Action, dev.DeviceType)
		}
	}
	return nil
}

// validateShape checks target and parameters without reference to a device.
func validateShape(spec ActionSpec, pr Proposal) error {
	if spec.NeedsTarget {
		if pr.Target == "" || !spec.Target.MatchString(pr.Target) {
			return domain.Errorf(domain.CategoryValidation, "action %s needs a valid target, got %q", spec.Name, pr.Target)
		}
	} else if pr.Target != "" {
		return domain.Errorf(domain.CategoryValidation, "action %s takes no target", spec.Name)
	}
	allowed := map[string]ParamSpec{}
	for _, ps := range spec.Params {
		allowed[ps.Name] = ps
	}
	for k, v := range pr.Params {
		ps, ok := allowed[k]
		if !ok {
			return domain.Errorf(domain.CategoryValidation, "action %s has no parameter %q", spec.Name, k)
		}
		if !ps.Pattern.MatchString(v) {
			return domain.Errorf(domain.CategoryValidation, "parameter %s of %s has an invalid value", k, spec.Name)
		}
	}
	for _, ps := range spec.Params {
		if ps.Required {
			if _, ok := pr.Params[ps.Name]; !ok {
				return domain.Errorf(domain.CategoryValidation, "action %s requires parameter %q", spec.Name, ps.Name)
			}
		}
	}
	if len(pr.Reason) > 240 {
		return domain.Errorf(domain.CategoryValidation, "reason is too long")
	}
	return nil
}

// expand substitutes {device} and event labels into a template string.
func expand(tmpl string, ev domain.Event) string {
	if !strings.Contains(tmpl, "{") {
		return tmpl
	}
	pairs := []string{"{device}", ev.DeviceID, "{event}", ev.Type}
	for k, v := range ev.Labels {
		pairs = append(pairs, "{"+k+"}", v)
	}
	return strings.NewReplacer(pairs...).Replace(tmpl)
}

func describe(pr Proposal) string {
	if pr.Target != "" {
		return fmt.Sprintf("%s(%s)", pr.Action, pr.Target)
	}
	return pr.Action
}
