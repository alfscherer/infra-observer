package domain

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestStableIDIsDeterministicAndSeparatorSafe(t *testing.T) {
	if StableID("a", "b") != StableID("a", "b") {
		t.Fatal("same input must give same id")
	}
	if StableID("ab", "c") == StableID("a", "bc") {
		t.Fatal("part boundaries must be significant")
	}
}

func TestLabelsKeyIsCanonical(t *testing.T) {
	a := LabelsKey(map[string]string{"b": "2", "a": "1"})
	if a != "a=1,b=2" {
		t.Fatalf("got %q", a)
	}
	if LabelsKey(nil) != "" {
		t.Fatal("nil labels must render empty")
	}
}

func TestObservationIDStableAcrossRedelivery(t *testing.T) {
	l := map[string]string{"interface": "Gi0/1"}
	if NewObservationID("c1", "sw", "m", l) != NewObservationID("c1", "sw", "m", l) {
		t.Fatal("redelivery must reproduce the id")
	}
	if NewObservationID("c1", "sw", "m", l) == NewObservationID("c2", "sw", "m", l) {
		t.Fatal("a new collection cycle must produce a new id")
	}
}

func TestCategoryOf(t *testing.T) {
	cases := []struct {
		err       error
		cat       Category
		retryable bool
	}{
		{Errorf(CategoryValidation, "bad"), CategoryValidation, false},
		{Wrap(CategoryDependency, "db", errors.New("down")), CategoryDependency, true},
		{fmt.Errorf("outer: %w", Errorf(CategoryTimeout, "slow")), CategoryTimeout, true},
		{context.DeadlineExceeded, CategoryTimeout, true},
		{errors.New("mystery"), CategoryPermanent, false},
		{Errorf(CategoryScript, "boom"), CategoryScript, false},
	}
	for _, c := range cases {
		if got := CategoryOf(c.err); got != c.cat {
			t.Errorf("%v: category %q, want %q", c.err, got, c.cat)
		}
		if got := IsRetryable(c.err); got != c.retryable {
			t.Errorf("%v: retryable %v, want %v", c.err, got, c.retryable)
		}
	}
	if Wrap(CategoryTransient, "x", nil) != nil {
		t.Fatal("wrapping nil must stay nil")
	}
}

func TestDeviceValidate(t *testing.T) {
	ok := Device{ID: "switch-01", Hostname: "switch-01.lab", ManagementAddress: "10.0.0.1", DeviceType: DeviceSwitch, Enabled: true}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid device rejected: %v", err)
	}
	bad := []Device{
		{},
		{ID: "a b", Hostname: "h", ManagementAddress: "x", DeviceType: DeviceSwitch},
		{ID: "a", Hostname: "h", ManagementAddress: "x", DeviceType: "toaster"},
		{ID: "a", Hostname: "h", ManagementAddress: "x", DeviceType: DeviceServer, Collection: CollectionSpec{Protocol: "carrier-pigeon"}},
	}
	for i, d := range bad {
		err := d.Validate()
		if err == nil {
			t.Errorf("case %d: expected error", i)
		} else if CategoryOf(err) != CategoryValidation {
			t.Errorf("case %d: category %q", i, CategoryOf(err))
		}
	}
}

func TestObservationFloatAndClone(t *testing.T) {
	o := Observation{Value: true, Labels: map[string]string{"a": "1"}}
	if f, ok := o.Float(); !ok || f != 1 {
		t.Fatalf("bool should read as 1, got %v %v", f, ok)
	}
	c := o.Clone()
	c.Labels["a"] = "2"
	if o.Labels["a"] != "1" {
		t.Fatal("clone must not share label map")
	}
	if _, ok := (Observation{Value: "x"}).Float(); ok {
		t.Fatal("string is not numeric")
	}
}

func TestSeverityRank(t *testing.T) {
	if !(SeverityCritical.Rank() > SeverityWarning.Rank() && SeverityWarning.Rank() > SeverityInfo.Rank()) {
		t.Fatal("ranking broken")
	}
	if Severity("loud").Valid() {
		t.Fatal("unknown severity must be invalid")
	}
}
