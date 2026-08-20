// Package health separates three questions that are easy to conflate:
//
//	liveness    - is the process running at all? (restart it if not)
//	readiness   - can it do its job right now? (send it work only if so)
//	dependencies - what exactly is wrong, and is it critical?
//
// A process can be alive but unable to process messages because PostgreSQL or
// NATS is unavailable; that makes it unready, not dead, and restarting it would
// not help. A broken optional extension degrades the report without making the
// service unready.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Status of a check or of the whole report.
type Status string

const (
	OK       Status = "ok"
	Degraded Status = "degraded" // working, but something optional is wrong
	Down     Status = "down"
)

// Result is what a check returns.
type Result struct {
	Status Status `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// Check probes one dependency. Critical checks decide readiness.
type Check struct {
	Name     string
	Critical bool
	Fn       func(ctx context.Context) Result
}

// CheckResult is a Result with timing, as reported.
type CheckResult struct {
	Result
	Critical  bool    `json:"critical"`
	LatencyMS float64 `json:"latency_ms"`
}

// Report is the outcome of running every check.
type Report struct {
	Status  string                 `json:"status"` // ready, degraded, unready
	Ready   bool                   `json:"ready"`
	Checks  map[string]CheckResult `json:"checks"`
	Version string                 `json:"version,omitempty"`
	Uptime  string                 `json:"uptime"`
}

// Checker runs checks and serves the HTTP endpoints.
type Checker struct {
	Version string
	Timeout time.Duration // per check (default 2s)
	started time.Time
	checks  []Check
}

// NewChecker returns a Checker with the given checks.
func NewChecker(version string, checks ...Check) *Checker {
	return &Checker{Version: version, Timeout: 2 * time.Second, started: time.Now(), checks: checks}
}

// Run executes all checks concurrently, each under its own timeout. A check
// that hangs is reported down; it cannot hang the health endpoint.
func (c *Checker) Run(ctx context.Context) Report {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	results := make(map[string]CheckResult, len(c.checks))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, ch := range c.checks {
		wg.Add(1)
		go func(ch Check) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			start := time.Now()
			done := make(chan Result, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						done <- Result{Status: Down, Detail: "check panicked"}
					}
				}()
				done <- ch.Fn(cctx)
			}()
			var res Result
			select {
			case res = <-done:
			case <-cctx.Done():
				res = Result{Status: Down, Detail: "check timed out after " + timeout.String()}
			}
			mu.Lock()
			results[ch.Name] = CheckResult{Result: res, Critical: ch.Critical, LatencyMS: float64(time.Since(start).Microseconds()) / 1000}
			mu.Unlock()
		}(ch)
	}
	wg.Wait()

	rep := Report{Checks: results, Version: c.Version, Uptime: time.Since(c.started).Round(time.Second).String(), Ready: true, Status: "ready"}
	for _, r := range results {
		switch {
		case r.Status == Down && r.Critical:
			rep.Ready, rep.Status = false, "unready"
		case r.Status != OK && rep.Status == "ready":
			rep.Status = "degraded"
		}
	}
	return rep
}

// Names lists check names, sorted (for tests and docs).
func (c *Checker) Names() []string {
	var n []string
	for _, ch := range c.checks {
		n = append(n, ch.Name)
	}
	sort.Strings(n)
	return n
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// Liveness answers 200 while the process can serve a request at all. It
// deliberately checks no dependency.
func (c *Checker) Liveness() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive", "uptime": time.Since(c.started).Round(time.Second).String()})
	})
}

// Readiness answers 200 when every critical dependency is up, else 503. The
// body is the full report so an operator sees why.
func (c *Checker) Readiness() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rep := c.Run(r.Context())
		code := http.StatusOK
		if !rep.Ready {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, rep)
	})
}

// Dependencies always answers 200 with the detailed report: it is for humans
// and dashboards, not for load balancers.
func (c *Checker) Dependencies() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, c.Run(r.Context())) })
}

// Mount registers the standard endpoints on mux.
func (c *Checker) Mount(mux *http.ServeMux) {
	mux.Handle("/healthz", c.Liveness())
	mux.Handle("/readyz", c.Readiness())
	mux.Handle("/health/dependencies", c.Dependencies())
}
