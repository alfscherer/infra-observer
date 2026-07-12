// Package inventory holds the registered devices.
//
// The YAML file is the source of registration; the Registry is the in-process
// read model that enrichment, collectors and script host functions consult.
// PostgreSQL mirrors it for the API and history (see internal/persistence).
package inventory

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"sort"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// Query filters devices. Zero fields match everything.
type Query struct {
	Site       string
	DeviceType domain.DeviceType
	Tag        string
	Enabled    *bool
}

// Reader is the read-only view given to stages and script host APIs.
type Reader interface {
	Get(id string) (domain.Device, bool)
	List() []domain.Device
	Lookup(q Query) []domain.Device
}

// LoadFile parses and validates an inventory YAML file.
func LoadFile(path string) ([]domain.Device, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read inventory: %w", err)
	}
	devices, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("inventory %s: %w", path, err)
	}
	return devices, nil
}

// Parse decodes inventory YAML strictly (unknown keys are errors) and applies
// the one default that a zero value cannot express: enabled is true unless set.
func Parse(raw []byte) ([]domain.Device, error) {
	var doc struct {
		Devices []domain.Device `yaml:"devices"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	// Second, lenient pass only to learn which devices set `enabled` explicitly.
	var flags struct {
		Devices []struct {
			Enabled *bool `yaml:"enabled"`
		} `yaml:"devices"`
	}
	if err := yaml.Unmarshal(raw, &flags); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for i := range doc.Devices {
		if flags.Devices[i].Enabled == nil {
			doc.Devices[i].Enabled = true
		}
		d := doc.Devices[i]
		if err := d.Validate(); err != nil {
			return nil, err
		}
		if seen[d.ID] {
			return nil, domain.Errorf(domain.CategoryValidation, "duplicate device id %q", d.ID)
		}
		seen[d.ID] = true
	}
	return doc.Devices, nil
}

// Registry is a concurrency-safe in-memory Reader.
type Registry struct {
	mu      sync.RWMutex
	devices map[string]domain.Device
}

// NewRegistry builds a registry from validated devices.
func NewRegistry(devices []domain.Device) *Registry {
	r := &Registry{}
	r.Replace(devices)
	return r
}

// Replace swaps the whole device set, preserving known LastSeen values.
func (r *Registry) Replace(devices []domain.Device) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := make(map[string]domain.Device, len(devices))
	for _, d := range devices {
		if old, ok := r.devices[d.ID]; ok && d.LastSeen.IsZero() {
			d.LastSeen = old.LastSeen
		}
		next[d.ID] = d
	}
	r.devices = next
}

func (r *Registry) Get(id string) (domain.Device, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.devices[id]
	return clone(d), ok
}

func (r *Registry) List() []domain.Device { return r.Lookup(Query{}) }

func (r *Registry) Lookup(q Query) []domain.Device {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []domain.Device
	for _, d := range r.devices {
		if q.Site != "" && d.Site != q.Site {
			continue
		}
		if q.DeviceType != "" && d.DeviceType != q.DeviceType {
			continue
		}
		if q.Tag != "" && !d.HasTag(q.Tag) {
			continue
		}
		if q.Enabled != nil && d.Enabled != *q.Enabled {
			continue
		}
		out = append(out, clone(d))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Touch records that data was seen from a device. It never moves LastSeen backwards.
func (r *Registry) Touch(id string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.devices[id]; ok && at.After(d.LastSeen) {
		d.LastSeen = at
		r.devices[id] = d
	}
}

func clone(d domain.Device) domain.Device {
	d.Tags = slices.Clone(d.Tags)
	d.Capabilities = slices.Clone(d.Capabilities)
	if d.Attributes != nil {
		m := make(map[string]string, len(d.Attributes))
		for k, v := range d.Attributes {
			m[k] = v
		}
		d.Attributes = m
	}
	return d
}
