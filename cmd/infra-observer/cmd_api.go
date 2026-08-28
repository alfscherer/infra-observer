package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/alfscherer/infra-observer/internal/api"
	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/health"
	"github.com/alfscherer/infra-observer/internal/secrets"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

// cmdAPI serves the REST API. It needs only PostgreSQL: approvals are recorded
// in the database and picked up by the automation worker, so the API has no
// NATS dependency and stays up when the broker is down.
func cmdAPI(args []string) error {
	fs := flag.NewFlagSet("api", flag.ContinueOnError)
	path := configFlag(fs)
	migrate := fs.Bool("migrate", false, "apply pending database migrations before starting")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log := newLogger(cfg.Logging, "api")
	ctx, stop := signalContext()
	defer stop()

	m := telemetry.New()
	store, err := connectDatabase(ctx, cfg, log, *migrate)
	if err != nil {
		return err
	}
	defer store.Close()
	store.OnError = m.DatabaseError

	srv := &api.Server{
		Store: store, Approver: &automation.Engine{Store: store, Log: log}, Metrics: m, Log: log, Version: version,
		Health:          health.NewChecker(version, postgresCheck(store)),
		DefaultPageSize: cfg.API.DefaultPageSize, MaxPageSize: cfg.API.MaxPageSize,
	}
	if cfg.API.ApproversRef != "" {
		// Configured but unresolvable is a startup error, not a silent downgrade
		// to "approvals disabled": the operator asked for approvals.
		secret, err := secrets.EnvFile{Dir: cfg.Secrets.Dir}.Resolve(ctx, cfg.API.ApproversRef)
		if err != nil {
			return fmt.Errorf("api.approvers_ref: %w", err)
		}
		tokens := map[string]string(secret)
		if len(tokens) == 0 {
			return fmt.Errorf("api.approvers_ref %q holds no tokens", cfg.API.ApproversRef)
		}
		srv.Auth = api.TokenAuth{Tokens: tokens}
		log.Info("approvals enabled", "approvers", len(tokens))
	} else {
		log.Info("approvals disabled: set api.approvers_ref to enable POST /api/automation/{id}/approve")
	}

	httpSrv := &http.Server{
		Addr: cfg.API.Listen, Handler: srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.API.Listen)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log.Info("api shutting down")
	return httpSrv.Shutdown(sctx)
}
