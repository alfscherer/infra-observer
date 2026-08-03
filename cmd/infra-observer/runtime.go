package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/alfscherer/infra-observer/internal/config"
)

// signalContext is cancelled on SIGINT or SIGTERM: the shutdown path every
// long-running role shares.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// newLogger builds the structured logger. Every role logs JSON by default so
// logs can be filtered on fields (component, device_id, observation_id...).
func newLogger(cfg config.Logging, role string) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.Level))
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if cfg.Format == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(h).With("role", role)
}
