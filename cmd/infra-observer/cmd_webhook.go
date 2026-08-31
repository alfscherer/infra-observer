package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
)

// cmdWebhookSink runs a tiny receiver that stands in for a chat bridge or a
// ticketing intake, so the webhook integration and the send_webhook action can
// be exercised end to end in the lab. It records what it receives and serves it
// back on GET /v1/received. It is a lab tool, not a product.
func cmdWebhookSink(args []string) error {
	fs := flag.NewFlagSet("webhook-sink", flag.ContinueOnError)
	path := configFlag(fs)
	listen := fs.String("listen", envDefault("INFRA_OBSERVER_WEBHOOK_LISTEN", ":8090"), "listen address")
	token := fs.String("token", os.Getenv("INFRA_OBSERVER_WEBHOOK_TOKEN"), "bearer token required on POST (empty = none)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log := newLogger(cfg.Logging, "webhook-sink")

	var mu sync.Mutex
	var received []map[string]any
	seen := map[string]bool{}
	mux := http.NewServeMux()
	post := func(kind string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if *token != "" && r.Header.Get("Authorization") != "Bearer "+*token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			body, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
			var payload map[string]any
			if json.Unmarshal(body, &payload) != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			key := r.Header.Get("Idempotency-Key")
			mu.Lock()
			dup := key != "" && seen[key]
			if key != "" {
				seen[key] = true
			}
			if !dup {
				received = append(received, map[string]any{"kind": kind, "at": time.Now().UTC(), "idempotency_key": key, "payload": payload})
				if len(received) > 200 {
					received = received[1:]
				}
			}
			mu.Unlock()
			log.Info("received", "kind", kind, "duplicate", dup, "request_id", r.Header.Get("X-Request-Id"), "idempotency_key", key)
			w.WriteHeader(http.StatusAccepted)
		}
	}
	mux.HandleFunc("POST /v1/alerts", post("alert"))
	mux.HandleFunc("POST /v1/automation", post("automation"))
	mux.HandleFunc("GET /v1/received", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(received)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	ctx, stop := signalContext()
	defer stop()
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = srv.Close() }()
	log.Info("webhook sink listening", "addr", *listen, "token_required", *token != "")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
