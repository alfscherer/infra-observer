// Package scripting is the extension subsystem: it discovers scripts, runs
// them on an isolated pool of embedded JavaScript interpreters, and enforces
// the contracts between scripts and the rest of the platform.
//
// JavaScript here is an extension language. It can make the pipeline
// configurable at its edges; it is not a second application runtime and it
// has no way to reach the operating system, the database or NATS.
package scripting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
)

// ErrNotActive is returned when a call targets a script that is disabled,
// quarantined or failed. Callers treat it as "skip this extension".
var ErrNotActive = errors.New("script not active")

// Service owns the registry and the executor pool.
type Service struct {
	Registry *registry.Registry
	Pool     *runtime.Pool
	cfg      config.Scripting
	log      *slog.Logger

	// OnResult, if set, is told about every finished invocation (metrics hook).
	OnResult func(key string, d time.Duration, err error)
	// OnQuarantine, if set, is told when a script is quarantined.
	OnQuarantine func(key string)
}

// New builds the service and performs the initial load. A script that fails to
// load is reported, never fatal.
func New(cfg config.Scripting, settings map[string]map[string]config.ScriptSettings, installers []runtime.Installer, log *slog.Logger) (*Service, registry.Report) {
	if log == nil {
		log = slog.Default()
	}
	reg := registry.New(cfg.Directories, settings, cfg.QuarantineAfter)
	s := &Service{Registry: reg, cfg: cfg, log: log.With("component", "scripting")}
	s.Pool = runtime.NewPool(reg, runtime.Options{
		Workers: cfg.Workers, QueueSize: cfg.QueueSize, Timeout: cfg.ExecutionTimeout, Installers: installers,
	})
	return s, s.Reload()
}

// Close stops the executors.
func (s *Service) Close() { s.Pool.Close() }

// Reload rescans the script directories, validates each script by loading it
// in a scratch interpreter, and publishes the new set to the executors. Broken
// scripts are reported and excluded; the rest keep running.
func (s *Service) Reload() registry.Report {
	rep := s.Registry.Reload()
	for _, sc := range s.Registry.List() {
		if sc.Status != registry.StatusActive || sc.Program == nil {
			continue
		}
		raw, err := runtime.ReadExport(sc.Program, "meta", s.cfg.ExecutionTimeout*4)
		if err != nil {
			s.Registry.MarkFailed(sc.Key, err.Error())
			rep.Failed++
			rep.Loaded--
			rep.Errors = append(rep.Errors, sc.Key+": "+err.Error())
			continue
		}
		var meta struct {
			Version     string   `json:"version"`
			Description string   `json:"description"`
			Metrics     []string `json:"metrics"`
		}
		problem := ""
		switch {
		case json.Unmarshal(raw, &meta) != nil || meta.Version == "":
			problem = "meta.version is required"
		case sc.Kind == registry.KindTransform && len(meta.Metrics) == 0:
			// Transforms run for every matching observation. Making the metric
			// filter mandatory keeps "call JavaScript for everything" from ever
			// being the default.
			problem = "transform scripts must declare meta.metrics (metric names, or prefixes ending in *)"
		}
		if problem != "" {
			s.Registry.MarkFailed(sc.Key, problem)
			rep.Failed++
			rep.Loaded--
			rep.Errors = append(rep.Errors, sc.Key+": "+problem)
			continue
		}
		s.Registry.SetMeta(sc.Key, meta.Version, meta.Description, meta.Metrics)
	}
	sort.Strings(rep.Errors)
	for _, e := range rep.Errors {
		s.log.Warn("script not loaded", "error", e)
	}
	s.log.Info("scripts loaded", "loaded", rep.Loaded, "failed", rep.Failed, "disabled", rep.Disabled)
	return rep
}

// Watch reloads scripts when their files change, until ctx ends. It polls
// file metadata rather than depending on OS notifications: portable, simple,
// and cheap at the size of a script directory.
func (s *Service) Watch(ctx context.Context, interval time.Duration) {
	last := s.signature()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if sig := s.signature(); sig != last {
				last = sig
				s.log.Info("script change detected; reloading")
				s.Reload()
			}
		}
	}
}

func (s *Service) signature() string {
	var sig string
	for _, dir := range s.cfg.Directories {
		files, _ := filepath.Glob(filepath.Join(dir, "*", "*.js"))
		sort.Strings(files)
		for _, f := range files {
			if st, err := os.Stat(f); err == nil {
				sig += fmt.Sprintf("%s:%d:%d;", f, st.Size(), st.ModTime().UnixNano())
			}
		}
	}
	return sig
}

// Call invokes fn of the script identified by key (kind/id). data is opaque
// per-call context for host functions. Failures of the script (exceptions,
// deadlines, panics, bad output) count toward quarantine; platform conditions
// such as saturation or cancellation do not.
func (s *Service) Call(ctx context.Context, key, fn string, data any, args ...any) (json.RawMessage, error) {
	sc, ok := s.Registry.Get(key)
	if !ok || sc.Status != registry.StatusActive {
		return nil, fmt.Errorf("%w: %s", ErrNotActive, key)
	}
	start := time.Now()
	out, err := s.Pool.Call(ctx, sc.Program.ID, fn, data, args...)
	s.finish(key, time.Since(start), err)
	return out, err
}

// Failed lets a caller report that a script's *result* was invalid (it ran,
// but returned something the contract rejects). That is a script failure too.
func (s *Service) Failed(key string, err error) { s.finish(key, 0, err) }

func (s *Service) finish(key string, d time.Duration, err error) {
	if s.OnResult != nil {
		s.OnResult(key, d, err)
	}
	if err != nil && domain.CategoryOf(err) != domain.CategoryScript {
		return // saturation, cancellation: not the script's fault
	}
	if s.Registry.RecordResult(key, err) {
		s.log.Error("script quarantined", "script", key, "error", err)
		if s.OnQuarantine != nil {
			s.OnQuarantine(key)
		}
	}
}

// Health summarises the subsystem for readiness and the API.
type Health struct {
	Loaded      int           `json:"loaded"`
	Disabled    int           `json:"disabled"`
	Quarantined int           `json:"quarantined"`
	Failed      int           `json:"failed"`
	Stats       runtime.Stats `json:"stats"`
}

// Health returns current counts and executor statistics.
func (s *Service) Health() Health {
	c := s.Registry.Counts()
	return Health{
		Loaded: c[registry.StatusActive], Disabled: c[registry.StatusDisabled],
		Quarantined: c[registry.StatusQuarantined], Failed: c[registry.StatusFailed], Stats: s.Pool.Stats(),
	}
}
