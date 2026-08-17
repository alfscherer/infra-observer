// Package adapters implements the device-facing side of automation.
//
// An adapter performs catalog actions against one class of device and is the
// only place device-specific knowledge lives. The mock adapters here do not
// touch real hardware: mutating actions are carried out against the
// simulator through a Controller, so a "bounce interface" really changes the
// simulated port and the loop from alert to recovery closes. Read-only
// actions use genuine SNMP requests, the one real protocol-backed part.
package adapters

import (
	"context"
	"fmt"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/domain"
)

// Controller changes the state of a (simulated) device. A production adapter
// would implement the same operations with SNMP SET, NETCONF or a vendor API;
// nothing above this interface would change.
type Controller interface {
	// Do performs event on device (for example "interface-admin-down" with
	// args {"interface": "Gi0/2"}).
	Do(ctx context.Context, device, event string, args map[string]string) error
}

// Set routes an action to the adapter for the device type. Actions that are
// not about a particular device class (webhooks, event records, extensions,
// noop) always go to the generic adapter.
type Set struct {
	ByType  map[domain.DeviceType]automation.DeviceAdapter
	Generic automation.DeviceAdapter
}

var genericActions = map[string]bool{"send_webhook": true, "create_event_record": true, "invoke_extension": true, "noop": true}

func (s Set) For(dev domain.Device, action string) (automation.DeviceAdapter, error) {
	if genericActions[action] {
		if s.Generic == nil {
			return nil, domain.Errorf(domain.CategoryUnsupported, "no generic adapter configured")
		}
		return s.Generic, nil
	}
	if a, ok := s.ByType[dev.DeviceType]; ok {
		return a, nil
	}
	return nil, domain.Errorf(domain.CategoryUnsupported, "no adapter for %s devices", dev.DeviceType)
}

// caps builds a capability list.
func caps(names ...string) []automation.Capability {
	out := make([]automation.Capability, len(names))
	for i, n := range names {
		out[i] = automation.Capability(n)
	}
	return out
}

func unsupported(kind string, a automation.Action) error {
	return domain.Errorf(domain.CategoryUnsupported, "%s adapter does not implement %s", kind, a.Type)
}

func detail(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func statusWord(v float64) string {
	switch int(v) {
	case 1:
		return "up"
	case 2:
		return "down"
	case 3:
		return "testing"
	}
	return fmt.Sprintf("status-%d", int(v))
}
