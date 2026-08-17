package sim

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// Scenario is a named change to one simulated device.
type Scenario struct {
	Device string            `json:"device"`
	Event  string            `json:"event"`
	Args   map[string]string `json:"args,omitempty"`
	// Duration, when positive, schedules the inverse event automatically.
	Duration time.Duration `json:"duration,omitempty"`
}

// inverse maps a disruptive event to the one that undoes it.
var inverse = map[string]string{
	"interface-down": "interface-up",
	"high-cpu":       "cpu-normal",
	"offline":        "online",
	"overheat":       "cool",
	"power-loss":     "power-restore",
	"auth-failure":   "auth-ok",
	"packet-errors":  "errors-clear",
}

// Events lists the scenario events a device type supports.
func Events(t domain.DeviceType) []string {
	ev := []string{"offline", "online", "auth-failure", "auth-ok", "reset", "high-cpu", "cpu-normal", "overheat", "cool"}
	switch t {
	case domain.DeviceSwitch, domain.DeviceRouter, domain.DeviceAccessPoint:
		ev = append(ev, "interface-down", "interface-up", "interface-flap", "interface-admin-down", "interface-admin-up", "packet-errors", "errors-clear")
	case domain.DeviceUPS:
		ev = append(ev, "power-loss", "power-restore")
	}
	sort.Strings(ev)
	return ev
}

// Control adapts the world to the Controller interface.
func (w *World) Control() Controller { return worldController{w} }

type worldController struct{ w *World }

func (c worldController) Apply(_ context.Context, s Scenario) (string, error) { return c.w.Apply(s) }

// Apply performs a scenario. It returns a human-readable description.
func (w *World) Apply(s Scenario) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	d, ok := w.devices[s.Device]
	if !ok {
		return "", domain.Errorf(domain.CategoryValidation, "unknown simulated device %q", s.Device)
	}
	supported := false
	for _, e := range Events(d.Type) {
		if e == s.Event {
			supported = true
		}
	}
	if !supported {
		return "", domain.Errorf(domain.CategoryUnsupported, "device %s (%s) does not support event %q; supported: %v", d.ID, d.Type, s.Event, Events(d.Type))
	}
	msg, err := w.applyLocked(d, s.Event, s.Args)
	if err != nil {
		return "", err
	}
	if s.Duration > 0 {
		if inv, ok := inverse[s.Event]; ok {
			args := s.Args
			w.schedule(w.opts.Now().Add(s.Duration), func() { _, _ = w.applyLocked(d, inv, args) })
			msg += fmt.Sprintf(" (reverts with %s after %s)", inv, s.Duration)
		}
	}
	return msg, nil
}

func (w *World) applyLocked(d *Device, event string, args map[string]string) (string, error) {
	switch event {
	case "offline":
		d.Offline = true
		return d.ID + " stopped answering SNMP", nil
	case "online":
		d.Offline = false
		return d.ID + " is answering SNMP again", nil
	case "auth-failure":
		d.AuthFail = true
		return d.ID + " now rejects credentials (authorizationError)", nil
	case "auth-ok":
		d.AuthFail = false
		return d.ID + " accepts credentials again", nil
	case "high-cpu":
		v := 97.0
		d.cpuOverride = &v
		return d.ID + " CPU pinned near 97%", nil
	case "cpu-normal":
		d.cpuOverride = nil
		return d.ID + " CPU back to baseline", nil
	case "overheat":
		v := 82.0
		d.tempOverride = &v
		return d.ID + " temperature rising to ~82C", nil
	case "cool":
		d.tempOverride = nil
		return d.ID + " temperature back to baseline", nil
	case "power-loss":
		d.OnBatt = true
		return d.ID + " lost mains power and is on battery", nil
	case "power-restore":
		d.OnBatt = false
		return d.ID + " mains power restored", nil
	case "reset":
		d.Offline, d.AuthFail, d.OnBatt, d.cpuOverride, d.tempOverride, d.flap = false, false, false, nil, nil, nil
		for _, i := range d.Iface {
			i.OperUp, i.AdminUp, i.errRate = true, true, 0
		}
		return d.ID + " reset to healthy baseline", nil
	}
	// interface scenarios
	i, err := pickIface(d, args["interface"])
	if err != nil {
		return "", err
	}
	switch event {
	case "interface-down":
		i.OperUp = false
		if d.flap != nil && d.flap.iface == i {
			d.flap = nil
		}
		return fmt.Sprintf("%s %s is down", d.ID, i.Name), nil
	case "interface-up":
		i.OperUp = true
		return fmt.Sprintf("%s %s is up", d.ID, i.Name), nil
	case "interface-admin-down":
		i.AdminUp = false
		return fmt.Sprintf("%s %s is administratively disabled", d.ID, i.Name), nil
	case "interface-admin-up":
		i.AdminUp, i.OperUp = true, true
		return fmt.Sprintf("%s %s is administratively enabled", d.ID, i.Name), nil
	case "interface-flap":
		toggles, interval := 14, 8*time.Second
		if v, err := strconv.Atoi(args["toggles"]); err == nil && v > 0 {
			toggles = v
		}
		if v, err := time.ParseDuration(args["interval"]); err == nil && v > 0 {
			interval = v
		}
		d.flap = &flapState{iface: i, remaining: toggles, interval: interval, next: w.opts.Now().Add(interval)}
		return fmt.Sprintf("%s %s will flap %d times, every %s", d.ID, i.Name, toggles, interval), nil
	case "packet-errors":
		i.errRate = 40
		return fmt.Sprintf("%s %s is accumulating errors", d.ID, i.Name), nil
	case "errors-clear":
		i.errRate = 0
		return fmt.Sprintf("%s %s stopped accumulating errors", d.ID, i.Name), nil
	}
	return "", domain.Errorf(domain.CategoryUnsupported, "unhandled event %q", event)
}

func pickIface(d *Device, name string) (*Iface, error) {
	if len(d.Iface) == 0 {
		return nil, domain.Errorf(domain.CategoryUnsupported, "device %s has no interfaces", d.ID)
	}
	if name == "" {
		return d.Iface[0], nil
	}
	for _, i := range d.Iface {
		if i.Name == name {
			return i, nil
		}
	}
	return nil, domain.Errorf(domain.CategoryValidation, "device %s has no interface %q", d.ID, name)
}
