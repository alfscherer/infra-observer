package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/persistence"
)

// deviceKey routes a message to a worker shard by device, so messages about
// one device are handled in order while different devices run in parallel.
func deviceKey(m messaging.Message) string {
	var p struct {
		DeviceID string `json:"device_id"`
	}
	if json.Unmarshal(m.Data(), &p) == nil && p.DeviceID != "" {
		return p.DeviceID
	}
	return m.Subject()
}

// workerOptions builds worker settings from configuration.
func workerOptions(cfg config.Config, log *slog.Logger, name, stream, durable, filter string) messaging.WorkerOptions {
	p := cfg.Processing
	return messaging.WorkerOptions{
		Name: name, Stream: stream, Durable: durable, FilterSubject: filter,
		Shards: p.Workers, QueueSize: p.QueueSize, MaxDeliver: p.MaxDeliver, AckWait: p.AckWait, RetryDelay: p.RetryDelay,
		KeyFunc: deviceKey, Log: log,
	}
}

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
