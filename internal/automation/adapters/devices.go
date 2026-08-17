package adapters

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/domain"
)

// ---- switch -------------------------------------------------------------------

// Switch is the mock adapter for switches and routers.
type Switch struct {
	Ctl    Controller
	Reader *Reader // optional: enables real SNMP preflight and queries

	mu           sync.Mutex
	descriptions map[string]string // mock configuration state: device/interface -> description
}

func (s *Switch) Capabilities(context.Context, domain.Device) ([]automation.Capability, error) {
	return caps("interface-admin", "interface-query"), nil
}

func (s *Switch) Execute(ctx context.Context, dev domain.Device, a automation.Action) (automation.ActionResult, error) {
	switch a.Type {
	case "query_interface_state":
		st, err := s.interfaceState(ctx, dev, a.Target)
		if err != nil {
			return automation.ActionResult{}, err
		}
		return automation.ActionResult{
			Message: fmt.Sprintf("%s %s is operationally %s, administratively %s", dev.ID, a.Target, st.Oper, st.Admin),
			Details: detail("interface", a.Target, "oper_status", st.Oper, "admin_status", st.Admin, "if_index", st.Index, "source", "snmp"),
		}, nil
	case "query_vlan_state":
		return automation.ActionResult{Message: "VLAN state is not modelled by the simulator; mock response", Details: detail("vlans", "1,10,20", "source", "mock")}, nil
	case "disable_interface", "enable_interface", "bounce_interface", "set_interface_description":
		return s.mutate(ctx, dev, a)
	}
	return automation.ActionResult{}, unsupported("switch", a)
}

// interfaceState reads through SNMP when a Reader is configured; the mock
// falls back to a fixed answer only when no Reader is available.
func (s *Switch) interfaceState(ctx context.Context, dev domain.Device, iface string) (InterfaceState, error) {
	if s.Reader == nil {
		return InterfaceState{Name: iface, Oper: "unknown", Admin: "unknown"}, nil
	}
	return s.Reader.Interface(ctx, dev, iface)
}

func (s *Switch) mutate(ctx context.Context, dev domain.Device, a automation.Action) (automation.ActionResult, error) {
	// Preflight: read the port first. A dry run stops here, so it verifies that
	// the target exists and reports what it would change, using only reads.
	st, err := s.interfaceState(ctx, dev, a.Target)
	if err != nil {
		return automation.ActionResult{}, err
	}
	d := detail("interface", a.Target, "before_oper", st.Oper, "before_admin", st.Admin)
	if a.DryRun {
		return automation.ActionResult{Message: fmt.Sprintf("dry-run: would %s %s on %s (currently oper=%s admin=%s)", strings.ReplaceAll(a.Type, "_", " "), a.Target, dev.ID, st.Oper, st.Admin), Details: d}, nil
	}
	args := map[string]string{"interface": a.Target}
	switch a.Type {
	case "disable_interface":
		err = s.Ctl.Do(ctx, dev.ID, "interface-admin-down", args)
	case "enable_interface":
		err = s.Ctl.Do(ctx, dev.ID, "interface-admin-up", args)
	case "bounce_interface":
		if err = s.Ctl.Do(ctx, dev.ID, "interface-admin-down", args); err == nil {
			err = s.Ctl.Do(ctx, dev.ID, "interface-admin-up", args)
		}
	case "set_interface_description":
		s.mu.Lock()
		if s.descriptions == nil {
			s.descriptions = map[string]string{}
		}
		key := dev.ID + "/" + a.Target
		d["before_description"] = s.descriptions[key]
		s.descriptions[key] = a.Params["description"]
		s.mu.Unlock()
		return automation.ActionResult{Message: fmt.Sprintf("set description of %s on %s", a.Target, dev.ID), Details: d, Changed: true}, nil
	}
	if err != nil {
		return automation.ActionResult{}, err
	}
	return automation.ActionResult{Message: fmt.Sprintf("%s completed for %s on %s", a.Type, a.Target, dev.ID), Details: d, Changed: true}, nil
}

// ---- access point -------------------------------------------------------------

// AccessPoint is the mock adapter for wireless access points.
type AccessPoint struct {
	Ctl    Controller
	Reader *Reader

	mu     sync.Mutex
	config map[string]string // mock configuration store: device/key -> value
}

func (p *AccessPoint) Capabilities(context.Context, domain.Device) ([]automation.Capability, error) {
	return caps("service-restart", "config-update", "client-query"), nil
}

func (p *AccessPoint) Execute(ctx context.Context, dev domain.Device, a automation.Action) (automation.ActionResult, error) {
	switch a.Type {
	case "query_ap_clients":
		if p.Reader == nil {
			return automation.ActionResult{Message: "client count unavailable without an SNMP reader", Details: detail("source", "mock")}, nil
		}
		n, err := p.Reader.Clients(ctx, dev)
		if err != nil {
			return automation.ActionResult{}, err
		}
		return automation.ActionResult{Message: fmt.Sprintf("%s has %d associated clients", dev.ID, n), Details: detail("clients", fmt.Sprint(n), "source", "snmp")}, nil
	case "query_interface_state":
		if p.Reader == nil {
			return automation.ActionResult{}, unsupported("access point (no reader)", a)
		}
		st, err := p.Reader.Interface(ctx, dev, a.Target)
		if err != nil {
			return automation.ActionResult{}, err
		}
		return automation.ActionResult{Message: fmt.Sprintf("%s %s is %s", dev.ID, a.Target, st.Oper), Details: detail("oper_status", st.Oper, "admin_status", st.Admin, "source", "snmp")}, nil
	case "restart_ap_service":
		if a.DryRun {
			return automation.ActionResult{Message: fmt.Sprintf("dry-run: would restart service %s on %s", a.Params["service"], dev.ID), Details: detail("service", a.Params["service"])}, nil
		}
		if err := p.Ctl.Do(ctx, dev.ID, "online", nil); err != nil { // a restart brings a wedged AP back
			return automation.ActionResult{}, err
		}
		return automation.ActionResult{Message: fmt.Sprintf("restarted service %s on %s", a.Params["service"], dev.ID), Details: detail("service", a.Params["service"]), Changed: true}, nil
	case "update_ap_config":
		key := dev.ID + "/" + a.Params["key"]
		p.mu.Lock()
		defer p.mu.Unlock()
		before := p.config[key]
		if a.DryRun {
			return automation.ActionResult{Message: fmt.Sprintf("dry-run: would set %s=%s on %s", a.Params["key"], a.Params["value"], dev.ID), Details: detail("before", before)}, nil
		}
		if p.config == nil {
			p.config = map[string]string{}
		}
		p.config[key] = a.Params["value"]
		return automation.ActionResult{Message: fmt.Sprintf("set %s on %s", a.Params["key"], dev.ID), Details: detail("before", before, "after", a.Params["value"], "source", "mock"), Changed: true}, nil
	}
	return automation.ActionResult{}, unsupported("access point", a)
}

// ---- server / workstation -------------------------------------------------------

// Host is the mock adapter for servers and workstations. It stands in for a
// management agent: restarting a service and running maintenance tasks are
// modelled; diagnostics are gathered with real SNMP reads.
type Host struct {
	Ctl    Controller
	Reader *Reader
	// Tasks are the predefined maintenance tasks. A task not listed here cannot
	// run, whatever a policy or script asks for.
	Tasks map[string]string
}

// DefaultTasks is the built-in catalogue of maintenance tasks.
func DefaultTasks() map[string]string {
	return map[string]string{
		"rotate-logs":   "rotate and compress application logs",
		"clear-tmp":     "remove stale files from the temporary directory",
		"refresh-cache": "rebuild the package metadata cache",
	}
}

func (h *Host) Capabilities(context.Context, domain.Device) ([]automation.Capability, error) {
	return caps("service-restart", "diagnostics", "maintenance"), nil
}

func (h *Host) Execute(ctx context.Context, dev domain.Device, a automation.Action) (automation.ActionResult, error) {
	switch a.Type {
	case "restart_service":
		svc := a.Params["service"]
		if a.DryRun {
			return automation.ActionResult{Message: fmt.Sprintf("dry-run: would restart service %s on %s", svc, dev.ID), Details: detail("service", svc)}, nil
		}
		if err := h.Ctl.Do(ctx, dev.ID, "online", nil); err != nil { // restarting the agent restores a wedged host
			return automation.ActionResult{}, err
		}
		return automation.ActionResult{Message: fmt.Sprintf("restarted service %s on %s", svc, dev.ID), Details: detail("service", svc), Changed: true}, nil
	case "collect_diagnostics":
		if h.Reader == nil {
			return automation.ActionResult{Message: "no diagnostics available without an SNMP reader", Details: detail("source", "mock")}, nil
		}
		f, err := h.Reader.Host(ctx, dev)
		if err != nil {
			return automation.ActionResult{}, err
		}
		return automation.ActionResult{
			Message: fmt.Sprintf("collected diagnostics from %s: up %ds, cpu %s%%, memory %s%%", dev.ID, f.UptimeSecs, strings.Join(f.CPUPercent, "/"), f.MemPercent),
			Details: detail("hostname", f.Name, "uptime_seconds", fmt.Sprint(f.UptimeSecs), "cpu_percent", strings.Join(f.CPUPercent, ","), "memory_percent", f.MemPercent, "source", "snmp"),
		}, nil
	case "run_maintenance_task":
		tasks := h.Tasks
		if tasks == nil {
			tasks = DefaultTasks()
		}
		desc, ok := tasks[a.Params["task"]]
		if !ok {
			names := make([]string, 0, len(tasks))
			for n := range tasks {
				names = append(names, n)
			}
			sort.Strings(names)
			return automation.ActionResult{}, domain.Errorf(domain.CategoryUnsupported, "task %q is not a predefined maintenance task (%s)", a.Params["task"], strings.Join(names, ", "))
		}
		if a.DryRun {
			return automation.ActionResult{Message: fmt.Sprintf("dry-run: would run %s (%s) on %s", a.Params["task"], desc, dev.ID), Details: detail("task", a.Params["task"])}, nil
		}
		return automation.ActionResult{Message: fmt.Sprintf("ran %s (%s) on %s", a.Params["task"], desc, dev.ID), Details: detail("task", a.Params["task"], "source", "mock"), Changed: true}, nil
	}
	return automation.ActionResult{}, unsupported("host", a)
}
