package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/health"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

func TestRunRejectsUnknownCommands(t *testing.T) {
	if err := run([]string{"frobnicate"}); err == nil {
		t.Fatal("unknown commands must fail")
	}
	if err := run([]string{"deadletter"}); err == nil {
		t.Fatal("deadletter needs a subcommand")
	}
	if err := run([]string{"script"}); err == nil {
		t.Fatal("script needs a subcommand")
	}
}

func TestObservabilityServerExposesMetricsAndHealth(t *testing.T) {
	cfg := config.Default()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	cfg.Observability.Listen = addr

	m := telemetry.New()
	m.DatabaseError(domain.Errorf(domain.CategoryDependency, "x"))
	healthy := true
	checker := health.NewChecker("test",
		health.Check{Name: "postgres", Critical: true, Fn: func(context.Context) health.Result {
			if healthy {
				return health.Result{Status: health.OK}
			}
			return health.Result{Status: health.Down, Detail: "connection refused"}
		}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveObservability(ctx, cfg, slog.New(slog.DiscardHandler), m, checker)

	fetch := func(path string) (int, string) {
		var lastErr error
		for i := 0; i < 50; i++ {
			resp, err := http.Get("http://" + addr + path)
			if err == nil {
				defer resp.Body.Close()
				b, _ := io.ReadAll(resp.Body)
				return resp.StatusCode, string(b)
			}
			lastErr = err
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("listener never came up: %v", lastErr)
		return 0, ""
	}
	if code, body := fetch("/metrics"); code != 200 || !strings.Contains(body, `database_errors_total{category="dependency"} 1`) {
		t.Fatalf("%d %s", code, body)
	}
	if code, _ := fetch("/healthz"); code != 200 {
		t.Fatal("liveness")
	}
	if code, _ := fetch("/readyz"); code != 200 {
		t.Fatal("ready")
	}
	healthy = false
	if code, body := fetch("/readyz"); code != 503 || !strings.Contains(body, "connection refused") {
		t.Fatalf("a critical dependency failure must make readiness 503 with the reason: %d %s", code, body)
	}
	if code, _ := fetch("/healthz"); code != 200 {
		t.Fatal("liveness must not depend on dependencies")
	}

	// an empty listen address disables the listener without error
	cfg.Observability.Listen = ""
	serveObservability(ctx, cfg, slog.New(slog.DiscardHandler), m, checker)
}
