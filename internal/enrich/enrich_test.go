package enrich

import (
	"errors"
	"testing"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
)

func reg() *inventory.Registry {
	return inventory.NewRegistry([]domain.Device{
		{ID: "sw1", Hostname: "sw1.lab", ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Vendor: "acme", Model: "m1", Site: "lab",
			Tags: []string{"core", "automation-enabled"}, Enabled: true,
			Attributes: map[string]string{"interface.Gi0/1.description": "uplink"}},
		{ID: "off", Hostname: "off", ManagementAddress: "x", DeviceType: domain.DeviceServer, Enabled: false},
	})
}

func TestEnrichAttachesContextWithoutTouchingLabels(t *testing.T) {
	e := Enricher{Inventory: reg()}
	in := domain.Observation{DeviceID: "sw1", Labels: map[string]string{"interface": "Gi0/1"}, Metadata: map[string]string{"raw_metric": "x"}}
	out, dev, err := e.Enrich(in)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"hostname": "sw1.lab", "device_type": "switch", "site": "lab", "vendor": "acme",
		"model": "m1", "tags": "core,automation-enabled", "interface_description": "uplink", "raw_metric": "x"}
	for k, v := range want {
		if out.Metadata[k] != v {
			t.Errorf("metadata[%s] = %q, want %q", k, out.Metadata[k], v)
		}
	}
	if len(out.Labels) != 1 || out.Labels["interface"] != "Gi0/1" {
		t.Fatalf("labels must be untouched: %v", out.Labels)
	}
	if dev.ID != "sw1" {
		t.Fatal("device not returned for downstream stages")
	}
	if in.Metadata["hostname"] != "" {
		t.Fatal("input mutated")
	}
}

func TestEnrichUnknownAndDisabledDevices(t *testing.T) {
	e := Enricher{Inventory: reg()}
	if _, _, err := e.Enrich(domain.Observation{DeviceID: "ghost"}); domain.CategoryOf(err) != domain.CategoryValidation {
		t.Fatalf("unknown device: %v", err)
	}
	if _, _, err := e.Enrich(domain.Observation{DeviceID: "off"}); !errors.Is(err, ErrDeviceDisabled) {
		t.Fatalf("disabled device: %v", err)
	}
}

func TestEnrichOmitsEmptyValues(t *testing.T) {
	e := Enricher{Inventory: reg()}
	out, _, _ := e.Enrich(domain.Observation{DeviceID: "sw1"})
	if _, present := out.Metadata["interface_description"]; present {
		t.Fatal("no interface label, so no interface description")
	}
}
