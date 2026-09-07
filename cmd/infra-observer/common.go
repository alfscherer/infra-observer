package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/persistence"
)

// connectDatabase opens PostgreSQL, retrying while it is not yet reachable so
// processes can start in any order. It optionally applies migrations.
func connectDatabase(ctx context.Context, cfg config.Config, log *slog.Logger, migrate bool) (*persistence.PGStore, error) {
	if err := cfg.RequireDatabase(); err != nil {
		return nil, err
	}
	var lastErr error
	for attempt := 0; attempt < 60; attempt++ {
		pool, err := persistence.Connect(ctx, cfg.Database.URL, cfg.Database.MaxConns)
		if err == nil {
			if migrate {
				if applied, merr := runMigrations(ctx, pool); merr != nil {
					pool.Close()
					return nil, merr
				} else if len(applied) > 0 {
					log.Info("migrations applied", "versions", applied)
				}
			}
			log.Info("connected to postgres")
			return persistence.NewPGStore(pool), nil
		}
		lastErr = err
		log.Warn("postgres not ready; retrying", "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil, fmt.Errorf("postgres unavailable: %w", lastErr)
}
