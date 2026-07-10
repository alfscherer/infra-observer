package domain

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// DeviceType is the coarse class of a device. Vendor detail deliberately does
// not live here; see Device.Attributes.
type DeviceType string

const (
	DeviceSwitch      DeviceType = "switch"
	DeviceAccessPoint DeviceType = "access_point"
	DeviceRouter      DeviceType = "router"
	DeviceServer      DeviceType = "server"
	DeviceWorkstation DeviceType = "workstation"
	DevicePrinter     DeviceType = "printer"
	DeviceUPS         DeviceType = "ups"
	DeviceGeneric     DeviceType = "generic"
)

var deviceTypes = []DeviceType{
	DeviceSwitch, DeviceAccessPoint, DeviceRouter, DeviceServer,
	DeviceWorkstation, DevicePrinter, DeviceUPS, DeviceGeneric,
}

// Valid reports whether t is a known device type.
func (t DeviceType) Valid() bool { return slices.Contains(deviceTypes, t) }

// DeviceTypes lists the supported device types.
func DeviceTypes() []DeviceType { return slices.Clone(deviceTypes) }

// CollectionSpec says how a device is observed. It is data for collectors and
// carries no credentials, only a reference.
type CollectionSpec struct {
	Protocol string        `json:"protocol" yaml:"protocol"`                   // "snmp" or "script"
	Profile  string        `json:"profile,omitempty" yaml:"profile,omitempty"` // SNMP profile id, or script id
	Interval time.Duration `json:"interval,omitempty" yaml:"interval,omitempty"`
}

// Device is the generic inventory record shared by every subsystem.
type Device struct {
	ID                string            `json:"id" yaml:"id"`
	Hostname          string            `json:"hostname" yaml:"hostname"`
	ManagementAddress string            `json:"management_address" yaml:"management_address"`
	DeviceType        DeviceType        `json:"device_type" yaml:"device_type"`
	Vendor            string            `json:"vendor,omitempty" yaml:"vendor,omitempty"`
	Model             string            `json:"model,omitempty" yaml:"model,omitempty"`
	SerialNumber      string            `json:"serial_number,omitempty" yaml:"serial_number,omitempty"`
	Site              string            `json:"site,omitempty" yaml:"site,omitempty"`
	Tags              []string          `json:"tags,omitempty" yaml:"tags,omitempty"`
	Capabilities      []string          `json:"capabilities,omitempty" yaml:"capabilities,omitempty"`
	CredentialsRef    string            `json:"credentials_ref,omitempty" yaml:"credentials_ref,omitempty"`
	Enabled           bool              `json:"enabled" yaml:"enabled"`
	LastSeen          time.Time         `json:"last_seen,omitzero" yaml:"-"`
	Collection        CollectionSpec    `json:"collection" yaml:"collection"`
	Attributes        map[string]string `json:"attributes,omitempty" yaml:"attributes,omitempty"`
}

// HasTag reports whether the device carries tag.
func (d Device) HasTag(tag string) bool { return slices.Contains(d.Tags, tag) }

// HasCapability reports whether the device advertises capability.
func (d Device) HasCapability(c string) bool { return slices.Contains(d.Capabilities, c) }

// Validate checks structural invariants. It does not touch the network.
func (d Device) Validate() error {
	var problems []string
	if d.ID == "" {
		problems = append(problems, "id is required")
	} else if strings.ContainsAny(d.ID, " \t\n/.*>") {
		problems = append(problems, "id must not contain whitespace, '/', '.', '*' or '>' (it is used in NATS subjects and URLs)")
	}
	if d.Hostname == "" {
		problems = append(problems, "hostname is required")
	}
	if d.ManagementAddress == "" {
		problems = append(problems, "management_address is required")
	}
	if !d.DeviceType.Valid() {
		problems = append(problems, fmt.Sprintf("device_type %q is not one of %v", d.DeviceType, deviceTypes))
	}
	if d.Collection.Protocol != "" && d.Collection.Protocol != "snmp" && d.Collection.Protocol != "script" {
		problems = append(problems, fmt.Sprintf("collection.protocol %q is not supported", d.Collection.Protocol))
	}
	if d.Collection.Interval < 0 {
		problems = append(problems, "collection.interval must not be negative")
	}
	if len(problems) > 0 {
		return Errorf(CategoryValidation, "device %q: %s", d.ID, strings.Join(problems, "; "))
	}
	return nil
}
