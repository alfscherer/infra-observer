// Package enrich attaches inventory context to observations.
//
// It runs after normalization because it reads mutable inventory: the same
// normalized observation replayed after an inventory change gets the new
// context, which is the desired behaviour for state and rules.
package enrich

import (
	"errors"
	"strings"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
)

// ErrDeviceDisabled means the device is registered but switched off; its
// observations are dropped without error.
var ErrDeviceDisabled = errors.New("device disabled")

// Enricher adds device context to Observation.Metadata. Context never goes
// into labels: labels identify a series, metadata describes it.
type Enricher struct {
	Inventory inventory.Reader
}

// Enrich returns o with context attached. An unregistered device is a
// validation error (poison); a disabled device returns ErrDeviceDisabled.
func (e Enricher) Enrich(o domain.Observation) (domain.Observation, domain.Device, error) {
	dev, ok := e.Inventory.Get(o.DeviceID)
	if !ok {
		return o, dev, domain.Errorf(domain.CategoryValidation, "observation %s references unregistered device %q", o.ObservationID, o.DeviceID)
	}
	if !dev.Enabled {
		return o, dev, ErrDeviceDisabled
	}
	out := o.Clone()
	if out.Metadata == nil {
		out.Metadata = map[string]string{}
	}
	set := func(k, v string) {
		if v != "" {
			out.Metadata[k] = v
		}
	}
	set("hostname", dev.Hostname)
	set("device_type", string(dev.DeviceType))
	set("site", dev.Site)
	set("vendor", dev.Vendor)
	set("model", dev.Model)
	set("tags", strings.Join(dev.Tags, ","))
	if iface := o.Labels["interface"]; iface != "" {
		set("interface_description", dev.Attributes["interface."+iface+".description"])
	}
	return out, dev, nil
}
