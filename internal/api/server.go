// Package api is the read-mostly REST interface to the platform.
//
// It exposes devices, derived state, metrics, events, alerts and automation
// history, plus one write: approving an automation request. It is a thin
// layer over the persistence read side and speaks its own DTOs, not the
// platform's internal messages.
package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/health"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

// Approver records an approval. The automation engine implements it.
type Approver interface {
	Approve(ctx context.Context, id, by string) error
}

// Authenticator identifies the caller of a privileged endpoint.
type Authenticator interface {
	// Identify returns the caller's identity, or false if the request carries no
	// valid credentials.
	Identify(r *http.Request) (string, bool)
}

// TokenAuth authenticates static bearer tokens mapped to identities. Tokens are
// compared in constant time and never logged; the identity is what ends up in
// the audit trail.
type TokenAuth struct{ Tokens map[string]string }

func (a TokenAuth) Identify(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	tok, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || tok == "" {
		return "", false
	}
	identity, found := "", false
	for t, id := range a.Tokens { // visit every token: no early exit that leaks which one matched
		if subtle.ConstantTimeCompare([]byte(t), []byte(tok)) == 1 {
			identity, found = id, true
		}
	}
	return identity, found
}

// Server is the HTTP API.
type Server struct {
	Store    persistence.Reader
	Approver Approver
	Auth     Authenticator // nil disables approvals
	Health   *health.Checker
	Metrics  *telemetry.Metrics
	Log      *slog.Logger
	Version  string

	DefaultPageSize int
	MaxPageSize     int
	RequestTimeout  time.Duration
}

// Handler returns the routed, instrumented handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/devices", s.listDevices)
	mux.HandleFunc("GET /api/devices/{id}", s.getDevice)
	mux.HandleFunc("GET /api/devices/{id}/state", s.getDeviceState)
	mux.HandleFunc("GET /api/devices/{id}/metrics", s.getDeviceMetrics)
	mux.HandleFunc("GET /api/events", s.listEvents)
	mux.HandleFunc("GET /api/alerts", s.listAlerts)
	mux.HandleFunc("GET /api/automation", s.listAutomation)
	mux.HandleFunc("GET /api/automation/{id}", s.getAutomation)
	mux.HandleFunc("POST /api/automation/{id}/approve", s.approve)
	if s.Health != nil {
		mux.Handle("GET /api/health", s.Health.Liveness())
		mux.Handle("GET /api/readiness", s.Health.Readiness())
		mux.Handle("GET /api/health/dependencies", s.Health.Dependencies())
	}
	if s.Metrics != nil {
		mux.Handle("GET /metrics", s.Metrics.Handler())
	}
	// Catch-all: distinguish "no such endpoint" (404) from "known endpoint, wrong
	// method" (405 with an Allow header), both in the API's JSON error shape.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var allow []string
		for _, m := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
			if m == r.Method {
				continue
			}
			probe := r.Clone(r.Context())
			probe.Method = m
			if _, pattern := mux.Handler(probe); pattern != "" && pattern != "/" {
				allow = append(allow, m)
			}
		}
		if len(allow) > 0 {
			w.Header().Set("Allow", strings.Join(allow, ", "))
			s.fail(w, r, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed for this endpoint")
			return
		}
		s.fail(w, r, http.StatusNotFound, "not_found", "no such endpoint")
	})
	return s.middleware(mux)
}

// --- responses -----------------------------------------------------------------

type errorBody struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id,omitempty"`
	} `json:"error"`
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message, b.Error.RequestID = code, msg, w.Header().Get("X-Request-Id")
	writeJSON(w, status, b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// failErr maps a categorised platform error onto an HTTP status. Internal
// error text is not echoed for server-side failures.
func (s *Server) failErr(w http.ResponseWriter, r *http.Request, err error) {
	switch domain.CategoryOf(err) {
	case domain.CategoryValidation:
		s.fail(w, r, http.StatusBadRequest, "invalid_request", err.Error())
	case domain.CategoryAuthentication:
		s.fail(w, r, http.StatusUnauthorized, "unauthorized", "authentication required")
	case domain.CategoryDependency, domain.CategoryTransient, domain.CategoryTimeout:
		s.log().Error("dependency failure", "request_id", w.Header().Get("X-Request-Id"), "path", r.URL.Path, "error", err)
		s.fail(w, r, http.StatusServiceUnavailable, "unavailable", "the service is temporarily unable to answer; try again shortly")
	default:
		s.log().Error("request failed", "request_id", w.Header().Get("X-Request-Id"), "path", r.URL.Path, "error", err)
		s.fail(w, r, http.StatusInternalServerError, "internal", "internal error")
	}
}

func (s *Server) log() *slog.Logger {
	if s.Log != nil {
		return s.Log.With("component", "api")
	}
	return slog.Default().With("component", "api")
}

// --- middleware ------------------------------------------------------------------

type statusWriter struct {
	http.ResponseWriter
	status int
	wrote  bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.status, w.wrote = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// middleware adds, outermost first: request id, panic recovery, a request
// deadline, a body size limit, structured access logging and metrics.
func (s *Server) middleware(next http.Handler) http.Handler {
	timeout := s.RequestTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Request-Id")
		if id == "" || len(id) > 64 {
			id = newRequestID()
		}
		rw.Header().Set("X-Request-Id", id)
		rw.Header().Set("X-Content-Type-Options", "nosniff")
		w := &statusWriter{ResponseWriter: rw, status: http.StatusOK}

		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		r = r.WithContext(ctx)
		r.Body = http.MaxBytesReader(w, r.Body, 4<<10)

		defer func() {
			if rec := recover(); rec != nil {
				s.log().Error("handler panicked", "request_id", id, "path", r.URL.Path, "panic", rec)
				if !w.wrote {
					s.fail(w, r, http.StatusInternalServerError, "internal", "internal error")
				}
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			if s.Metrics != nil {
				s.Metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(w.status)).Inc()
				s.Metrics.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
			}
			s.log().Info("request", "request_id", id, "method", r.Method, "route", route, "path", r.URL.Path, "status", w.status,
				"duration", time.Since(start), "remote", r.RemoteAddr)
		}()
		next.ServeHTTP(w, r)
	})
}
