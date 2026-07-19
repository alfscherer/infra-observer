// Package snmp implements the SNMP collection subsystem: configuration-driven
// profiles, a session abstraction over gosnmp, and a poller that turns
// responses into raw observations.
//
// Nothing here evaluates thresholds, updates health or triggers actions. The
// output of a poll is a list of observations, full stop.
package snmp

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// ValueType says how a raw SNMP value is decoded into an observation value.
type ValueType string

const (
	TypeInteger  ValueType = "integer"  // signed integer or enumeration -> number
	TypeGauge    ValueType = "gauge"    // Gauge32/Unsigned32 -> number
	TypeCounter  ValueType = "counter"  // Counter32/Counter64 -> number (monotonic)
	TypeDuration ValueType = "duration" // TimeTicks -> seconds
	TypeString   ValueType = "string"   // OctetString/OID -> string
)

// Metric is one thing a profile collects: a scalar OID or a table column.
type Metric struct {
	Name  string     `yaml:"name"` // raw, source-specific metric name
	OID   string     `yaml:"oid"`  // scalar instance OID, e.g. 1.3.6.1.2.1.1.3.0
	Table *TableSpec `yaml:"table"`
	Type  ValueType  `yaml:"type"`
}

// TableSpec describes a column walk. Each row becomes one observation whose
// labels come from the row index and from sibling columns.
type TableSpec struct {
	OID        string            `yaml:"oid"`         // column OID to walk
	IndexLabel string            `yaml:"index_label"` // label carrying the row index; default "index"
	Labels     map[string]string `yaml:"labels"`      // label name -> sibling column OID
}

// Profile is a named, configuration-driven set of metrics.
type Profile struct {
	ID          string   `yaml:"id"`
	Description string   `yaml:"description"`
	Include     []string `yaml:"include"`
	Match       struct {
		SysObjectIDPrefix string `yaml:"sysobjectid_prefix"`
	} `yaml:"match"`
	Metrics []Metric `yaml:"metrics"`
}

var oidPattern = regexp.MustCompile(`^\.?[0-9]+(\.[0-9]+)*$`)

func normOID(oid string) string { return strings.TrimPrefix(oid, ".") }

func (p *Profile) validate() error {
	var problems []string
	if p.ID == "" {
		problems = append(problems, "id is required")
	}
	seen := map[string]bool{}
	for i, m := range p.Metrics {
		where := fmt.Sprintf("metrics[%d] (%s)", i, m.Name)
		if m.Name == "" {
			problems = append(problems, fmt.Sprintf("metrics[%d]: name is required", i))
		}
		if m.Name != "" && seen[m.Name] {
			problems = append(problems, where+": duplicate metric name")
		}
		seen[m.Name] = true
		switch m.Type {
		case TypeInteger, TypeGauge, TypeCounter, TypeDuration, TypeString:
		default:
			problems = append(problems, fmt.Sprintf("%s: unknown type %q", where, m.Type))
		}
		switch {
		case (m.OID == "") == (m.Table == nil):
			problems = append(problems, where+": exactly one of oid or table is required")
		case m.OID != "" && !oidPattern.MatchString(m.OID):
			problems = append(problems, where+": malformed oid")
		case m.Table != nil:
			if !oidPattern.MatchString(m.Table.OID) {
				problems = append(problems, where+": malformed table.oid")
			}
			for l, o := range m.Table.Labels {
				if !oidPattern.MatchString(o) {
					problems = append(problems, fmt.Sprintf("%s: malformed label oid for %q", where, l))
				}
			}
		}
	}
	if len(problems) > 0 {
		return domain.Errorf(domain.CategoryValidation, "profile %q: %s", p.ID, strings.Join(problems, "; "))
	}
	return nil
}

// Profiles is the set of loaded profiles with includes already resolved.
type Profiles struct {
	byID map[string]Profile
}

// LoadProfiles reads every *.yaml file in dir.
func LoadProfiles(dir string) (*Profiles, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no SNMP profiles found in %s", dir)
	}
	sort.Strings(files)
	raws := make([][]byte, 0, len(files))
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		raws = append(raws, b)
	}
	return ParseProfiles(raws...)
}

// ParseProfiles decodes profile documents strictly and resolves includes.
func ParseProfiles(docs ...[]byte) (*Profiles, error) {
	raw := map[string]Profile{}
	for _, doc := range docs {
		var p Profile
		dec := yaml.NewDecoder(bytes.NewReader(doc))
		dec.KnownFields(true)
		if err := dec.Decode(&p); err != nil {
			return nil, domain.Errorf(domain.CategoryValidation, "parse profile: %v", err)
		}
		if err := p.validate(); err != nil {
			return nil, err
		}
		if _, dup := raw[p.ID]; dup {
			return nil, domain.Errorf(domain.CategoryValidation, "duplicate profile id %q", p.ID)
		}
		raw[p.ID] = p
	}
	out := &Profiles{byID: map[string]Profile{}}
	for id := range raw {
		flat, err := flatten(id, raw, nil)
		if err != nil {
			return nil, err
		}
		out.byID[id] = flat
	}
	return out, nil
}

// flatten merges includes depth-first; the including profile wins on name clashes.
func flatten(id string, raw map[string]Profile, stack []string) (Profile, error) {
	for _, s := range stack {
		if s == id {
			return Profile{}, domain.Errorf(domain.CategoryValidation, "profile include cycle: %s -> %s", strings.Join(stack, " -> "), id)
		}
	}
	p, ok := raw[id]
	if !ok {
		return Profile{}, domain.Errorf(domain.CategoryValidation, "profile %q not found (included from %v)", id, stack)
	}
	merged := map[string]Metric{}
	var order []string
	for _, inc := range p.Include {
		sub, err := flatten(inc, raw, append(stack, id))
		if err != nil {
			return Profile{}, err
		}
		for _, m := range sub.Metrics {
			if _, ok := merged[m.Name]; !ok {
				order = append(order, m.Name)
			}
			merged[m.Name] = m
		}
	}
	for _, m := range p.Metrics {
		if _, ok := merged[m.Name]; !ok {
			order = append(order, m.Name)
		}
		merged[m.Name] = m
	}
	p.Metrics = p.Metrics[:0:0]
	for _, n := range order {
		p.Metrics = append(p.Metrics, merged[n])
	}
	return p, nil
}

// Get returns a profile by id.
func (ps *Profiles) Get(id string) (Profile, bool) {
	p, ok := ps.byID[id]
	return p, ok
}

// IDs lists profile ids in sorted order.
func (ps *Profiles) IDs() []string {
	ids := make([]string, 0, len(ps.byID))
	for id := range ps.byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Match finds the profile with the longest sysObjectID prefix matching oid.
func (ps *Profiles) Match(sysObjectID string) (Profile, bool) {
	sysObjectID = normOID(sysObjectID)
	var best Profile
	bestLen := -1
	for _, p := range ps.byID {
		pre := normOID(p.Match.SysObjectIDPrefix)
		if pre == "" || !strings.HasPrefix(sysObjectID, pre) {
			continue
		}
		if len(pre) > bestLen {
			best, bestLen = p, len(pre)
		}
	}
	return best, bestLen >= 0
}
