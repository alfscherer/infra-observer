package inventory

import (
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

const sample = `
devices:
  - id: sw1
    hostname: sw1
    management_address: 10.0.0.1
    device_type: switch
    site: lab
    tags: [core]
  - id: srv1
    hostname: srv1
    management_address: 10.0.0.2
    device_type: server
    site: dc
    enabled: false
`

func TestParseDefaultsEnabledAndValidates(t *testing.T) {
	ds, err := Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if !ds[0].Enabled || ds[1].Enabled {
		t.Fatalf("enabled defaulting wrong: %v %v", ds[0].Enabled, ds[1].Enabled)
	}
}

func TestParseRejectsBadInput(t *testing.T) {
	for name, body := range map[string]string{
		"unknown key": "devices:\n  - id: a\n    hostname: a\n    management_address: x\n    device_type: switch\n    colour: red\n",
		"bad type":    "devices:\n  - id: a\n    hostname: a\n    management_address: x\n    device_type: toaster\n",
		"duplicate":   sample + "  - id: sw1\n    hostname: x\n    management_address: y\n    device_type: switch\n",
	} {
		if _, err := Parse([]byte(body)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestRegistryLookupAndIsolation(t *testing.T) {
	ds, _ := Parse([]byte(sample))
	r := NewRegistry(ds)
	if got := r.Lookup(Query{Site: "lab"}); len(got) != 1 || got[0].ID != "sw1" {
		t.Fatalf("site lookup: %v", got)
	}
	if got := r.Lookup(Query{Tag: "core"}); len(got) != 1 {
		t.Fatalf("tag lookup: %v", got)
	}
	f := false
	if got := r.Lookup(Query{Enabled: &f}); len(got) != 1 || got[0].ID != "srv1" {
		t.Fatalf("enabled lookup: %v", got)
	}
	d, _ := r.Get("sw1")
	d.Tags[0] = "mutated"
	if d2, _ := r.Get("sw1"); d2.Tags[0] != "core" {
		t.Fatal("callers must not be able to mutate registry state")
	}
	if _, ok := r.Get("nope"); ok {
		t.Fatal("unknown device must not be found")
	}
}

func TestTouchNeverMovesBackwardsAndSurvivesReplace(t *testing.T) {
	ds, _ := Parse([]byte(sample))
	r := NewRegistry(ds)
	now := time.Now()
	r.Touch("sw1", now)
	r.Touch("sw1", now.Add(-time.Hour))
	if d, _ := r.Get("sw1"); !d.LastSeen.Equal(now) {
		t.Fatal("LastSeen moved backwards")
	}
	r.Replace(ds)
	if d, _ := r.Get("sw1"); !d.LastSeen.Equal(now) {
		t.Fatal("LastSeen lost on reload")
	}
	_ = domain.DeviceSwitch
}

func TestShippedInventoryLoads(t *testing.T) {
	ds, err := LoadFile("../../configs/inventory.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 6 {
		t.Fatalf("expected the 6 lab devices, got %d", len(ds))
	}
}
