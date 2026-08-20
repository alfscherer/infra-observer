package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/health"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

// postgresCheck is critical: without the database nothing can be persisted.
func postgresCheck(store persistence.Store) health.Check {
	return health.Check{Name: "postgres", Critical: true, Fn: func(ctx context.Context) health.Result {
		if err := store.Ping(ctx); err != nil {
			return health.Result{Status: health.Down, Detail: err.Error()}
		}
		return health.Result{Status: health.OK}
	}}
}

// natsCheck is critical: without the broker no work arrives and no events leave.
func natsCheck(c *messaging.Client) health.Check {
	return health.Check{Name: "nats", Critical: true, Fn: func(ctx context.Context) health.Result {
		if err := c.Ping(ctx); err != nil {
			return health.Result{Status: health.Down, Detail: err.Error()}
		}
		return health.Result{Status: health.OK}
	}}
}

// scriptingCheck is deliberately NOT critical. The extension layer is
// optional: a quarantined or broken script degrades the report (so operators
// see it) but must not take the service out of rotation.
func scriptingCheck(svc *scripting.Service) health.Check {
	return health.Check{Name: "scripting", Critical: false, Fn: func(context.Context) health.Result {
		h := svc.Health()
		detail := fmt.Sprintf("%d active, %d disabled, %d quarantined, %d failed; %d timeouts, %d failed executions",
			h.Loaded, h.Disabled, h.Quarantined, h.Failed, h.Stats.Timeouts, h.Stats.Failures)
		if h.Quarantined > 0 || h.Failed > 0 {
			return health.Result{Status: health.Degraded, Detail: detail}
		}
		return health.Result{Status: health.OK, Detail: detail}
	}}
}

// serveObservability exposes /metrics and the health endpoints until ctx ends.
func serveObservability(ctx context.Context, cfg config.Config, log *slog.Logger, m *telemetry.Metrics, checker *health.Checker) {
	if cfg.Observability.Listen == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	checker.Mount(mux)
	srv := &http.Server{Addr: cfg.Observability.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("observability listener failed", "addr", cfg.Observability.Listen, "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("observability listening", "addr", cfg.Observability.Listen)
}
