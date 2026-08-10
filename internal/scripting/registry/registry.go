// Package registry discovers scripts on disk and manages their lifecycle:
// syntax validation, metadata, enable/disable from configuration, reload, and
// quarantine of scripts that keep failing.
//
// A script is identified by its kind (the directory it lives in) and its id
// (the file name without .js). The kind fixes the contract: the function the
// script must export and what it may return.
package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
)

// Kind of extension point, named after the directory that holds its scripts.
const (
	KindTransform   = "transforms"
	KindEnricher    = "enrichers"
	KindAutomation  = "automation"
	KindIntegration = "integrations"
	KindCollector   = "collectors"
)

// Contract maps a kind to the function a script of that kind must export.
var Contract = map[string]string{
	KindTransform:   "transform",
	KindEnricher:    "enrich",
	KindAutomation:  "proposeAutomation",
	KindIntegration: "handleEvent",
	KindCollector:   "collect",
}

// Status of a script.
type Status string

const (
	StatusActive      Status = "active"
	StatusDisabled    Status = "disabled"    // switched off in configuration
	StatusQuarantined Status = "quarantined" // failed too often in a row
	StatusFailed      Status = "failed"      // did not load or violates its contract
)

// Script is one discovered script and its runtime bookkeeping.
type Script struct {
	Key         string // kind/id
	Kind        string
	ID          string
	Path        string
	Version     string
	Description string
	Metrics     []string // metric names (or prefix*) the script handles; Go filters before calling JS
	Hash        string
	Status      Status
	Reason      string // why it is not active
	Config      map[string]any

	Program *runtime.Program

	ConsecutiveFailures int
	TotalFailures       uint64
	TotalCalls          uint64
	LastError           string
	LastFailureAt       time.Time
	QuarantinedAt       time.Time
}

// Report summarises a reload.
type Report struct {
	Loaded, Failed, Disabled int
	Errors                   []string
}

// Registry holds scripts. It implements runtime.Source.
type Registry struct {
	dirs      []string
	settings  map[string]map[string]config.ScriptSettings
	threshold int
	now       func() time.Time

	mu      sync.RWMutex
	scripts map[string]*Script
	gen     uint64
}

// New creates a registry. threshold is the number of consecutive failures
// after which a script is quarantined.
func New(dirs []string, settings map[string]map[string]config.ScriptSettings, threshold int) *Registry {
	return &Registry{dirs: dirs, settings: settings, threshold: max(threshold, 1), now: time.Now, scripts: map[string]*Script{}}
}

// Generation increments whenever the set of active programs may have changed.
func (r *Registry) Generation() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.gen
}

// Programs returns the programs workers should load: active scripts only.
func (r *Registry) Programs() []*runtime.Program {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*runtime.Program
	for _, key := range r.sortedKeys() {
		if s := r.scripts[key]; s.Status == StatusActive && s.Program != nil {
			out = append(out, s.Program)
		}
	}
	return out
}

func (r *Registry) sortedKeys() []string {
	keys := make([]string, 0, len(r.scripts))
	for k := range r.scripts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Reload rescans the directories. Unchanged scripts keep their failure
// counters; a changed script starts fresh (an operator fixing a quarantined
// script simply saves the file). A broken script is recorded as failed and
// never affects the others.
func (r *Registry) Reload() Report {
	found := map[string]*Script{}
	var rep Report
	for _, dir := range r.dirs {
		for kind := range Contract {
			files, _ := filepath.Glob(filepath.Join(dir, kind, "*.js"))
			sort.Strings(files)
			for _, f := range files {
				s := r.loadFile(kind, f)
				found[s.Key] = s
			}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, s := range found {
		if old, ok := r.scripts[key]; ok && old.Hash == s.Hash && s.Status == StatusActive {
			// unchanged source: keep runtime bookkeeping and any quarantine
			s.ConsecutiveFailures, s.TotalFailures, s.TotalCalls = old.ConsecutiveFailures, old.TotalFailures, old.TotalCalls
			s.LastError, s.LastFailureAt = old.LastError, old.LastFailureAt
			if old.Status == StatusQuarantined {
				s.Status, s.Reason, s.QuarantinedAt = StatusQuarantined, old.Reason, old.QuarantinedAt
			}
		}
	}
	r.scripts = found
	r.gen++
	for _, s := range found {
		switch s.Status {
		case StatusActive, StatusQuarantined:
			rep.Loaded++
		case StatusDisabled:
			rep.Disabled++
		case StatusFailed:
			rep.Failed++
			rep.Errors = append(rep.Errors, fmt.Sprintf("%s: %s", s.Key, s.Reason))
		}
	}
	return rep
}

func (r *Registry) loadFile(kind, path string) *Script {
	id := strings.TrimSuffix(filepath.Base(path), ".js")
	s := &Script{Key: kind + "/" + id, Kind: kind, ID: id, Path: path, Status: StatusActive}
	if set, ok := r.settings[kind][id]; ok {
		s.Config = set.Config
		if !set.IsEnabled() {
			s.Status, s.Reason = StatusDisabled, "disabled in configuration"
		}
	}
	src, err := os.ReadFile(path)
	if err != nil {
		s.Status, s.Reason = StatusFailed, "read: "+err.Error()
		return s
	}
	sum := sha256.Sum256(src)
	s.Hash = hex.EncodeToString(sum[:8])
	if s.Status == StatusDisabled {
		return s // do not even compile a script that is switched off
	}
	if err := Validate(kind, id, string(src), s); err != nil {
		s.Status, s.Reason = StatusFailed, err.Error()
	}
	return s
}

// Validate compiles src and checks it against the kind's contract, filling in
// metadata on s (which may be nil). It executes nothing.
func Validate(kind, id, src string, s *Script) error {
	fn, ok := Contract[kind]
	if !ok {
		return fmt.Errorf("unknown script kind %q", kind)
	}
	prog, err := runtime.Compile(kind+"/"+id, src)
	if err != nil {
		return err
	}
	if !prog.HasExport(fn) {
		return fmt.Errorf("a %s script must export a function named %s", strings.TrimSuffix(kind, "s"), fn)
	}
	if !prog.HasExport("meta") {
		return fmt.Errorf("script must export `const meta = {version: \"...\", description: \"...\"}`")
	}
	if s != nil {
		s.Program = prog
	}
	return nil
}

// MarkFailed records that a script that compiled cannot actually be used
// (for example its top-level code throws when loaded).
func (r *Registry) MarkFailed(key, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.scripts[key]; ok {
		s.Status, s.Reason = StatusFailed, reason
		r.gen++
	}
}

// SetMeta records metadata read from a loaded script (called by the service
// after it evaluates `meta` in a scratch interpreter).
func (r *Registry) SetMeta(key, version, description string, metrics []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.scripts[key]; ok {
		s.Version, s.Description, s.Metrics = version, description, metrics
	}
}

// ByKind returns the active scripts of a kind in id order.
func (r *Registry) ByKind(kind string) []Script {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Script
	for _, key := range r.sortedKeys() {
		if s := r.scripts[key]; s.Kind == kind && s.Status == StatusActive {
			out = append(out, *s)
		}
	}
	return out
}

// Get returns a copy of one script.
func (r *Registry) Get(key string) (Script, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.scripts[key]
	if !ok {
		return Script{}, false
	}
	return *s, true
}

// List returns every discovered script, active or not, sorted by key.
func (r *Registry) List() []Script {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Script, 0, len(r.scripts))
	for _, key := range r.sortedKeys() {
		out = append(out, *r.scripts[key])
	}
	return out
}

// RecordResult updates counters after an invocation. It returns true when this
// failure caused the script to be quarantined.
func (r *Registry) RecordResult(key string, err error) (quarantined bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.scripts[key]
	if !ok {
		return false
	}
	s.TotalCalls++
	if err == nil {
		s.ConsecutiveFailures = 0
		return false
	}
	s.TotalFailures++
	s.ConsecutiveFailures++
	s.LastError, s.LastFailureAt = err.Error(), r.now()
	if s.Status == StatusActive && s.ConsecutiveFailures >= r.threshold {
		s.Status = StatusQuarantined
		s.Reason = fmt.Sprintf("quarantined after %d consecutive failures; last: %s", s.ConsecutiveFailures, err)
		s.QuarantinedAt = r.now()
		r.gen++ // workers drop the script on their next call
		return true
	}
	return false
}

// SetEnabled is the operator switch: it disables a script or clears its
// quarantine and re-enables it. It returns false if the script is unknown or
// failed to load.
func (r *Registry) SetEnabled(key string, enabled bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.scripts[key]
	if !ok || s.Program == nil {
		return false
	}
	switch {
	case enabled && s.Status != StatusActive:
		s.Status, s.Reason, s.ConsecutiveFailures = StatusActive, "", 0
	case !enabled && s.Status == StatusActive:
		s.Status, s.Reason = StatusDisabled, "disabled by operator"
	default:
		return true
	}
	r.gen++
	return true
}

// Counts returns how many scripts are in each status.
func (r *Registry) Counts() map[Status]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	c := map[Status]int{}
	for _, s := range r.scripts {
		c[s.Status]++
	}
	return c
}
