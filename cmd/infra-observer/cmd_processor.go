package main

import (
	"flag"
	"time"

	"github.com/alfscherer/infra-observer/internal/app"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/enrich"
	"github.com/alfscherer/infra-observer/internal/health"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/normalize"
	"github.com/alfscherer/infra-observer/internal/pipeline"
	"github.com/alfscherer/infra-observer/internal/rules"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/state"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

// cmdProcessor runs the processing pipeline:
//
//	telemetry.raw.*      -> stage A (validate, normalize, transform scripts) -> telemetry.normalized
//	telemetry.normalized -> stage B (enrich, state, rules, persist)          -> outbox -> events.*
//
// plus the outbox relay, the stale-data sweeper and the inventory consumer. The
// assembly itself lives in internal/app so tests run the same wiring.
func cmdProcessor(args []string) error {
	fs := flag.NewFlagSet("processor", flag.ContinueOnError)
	path := configFlag(fs)
	migrate := fs.Bool("migrate", false, "apply pending database migrations before starting")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log := newLogger(cfg.Logging, "processor")
	ctx, stop := signalContext()
	defer stop()

	devices, err := inventory.LoadFile(cfg.Inventory.File)
	if err != nil {
		return err
	}
	registry := inventory.NewRegistry(devices)
	norm, err := normalize.LoadFile(cfg.Processing.NormalizationFile)
	if err != nil {
		return err
	}
	defs, err := state.LoadFile(cfg.Processing.StatesFile)
	if err != nil {
		return err
	}
	rs, err := rules.LoadFile(cfg.Processing.RulesFile)
	if err != nil {
		return err
	}

	m := telemetry.New()
	store, err := connectDatabase(ctx, cfg, log, *migrate)
	if err != nil {
		return err
	}
	defer store.Close()
	store.OnError = m.DatabaseError
	if err := store.UpsertDevices(ctx, devices); err != nil {
		return err
	}
	client, err := connectNATS(ctx, cfg, "processor", log)
	if err != nil {
		return err
	}
	defer client.Close()

	checks := []health.Check{postgresCheck(store), natsCheck(client)}
	proc := &pipeline.Processor{
		Validator: schema.NewValidator(), Normalizer: norm, Enricher: enrich.Enricher{Inventory: registry},
		States: defs, Rules: rs, Store: store, Log: log,
		StaleAfter: cfg.Processing.StaleAfter, SweepInterval: cfg.Processing.SweepInterval, StartedAt: time.Now(),
	}
	if cfg.Scripting.Enabled {
		svc, rep := scripting.New(cfg.Scripting, cfg.Scripts,
			[]runtime.Installer{api.Installer(api.Deps{Inventory: registry, State: store, Log: log})}, log)
		defer svc.Close()
		log.Info("scripting enabled", "loaded", rep.Loaded, "failed", rep.Failed, "disabled", rep.Disabled)
		proc.Ext = &scripting.Extensions{Svc: svc, Validator: proc.Validator, Log: log}
		m.AttachScripting(svc)
		checks = append(checks, scriptingCheck(svc))
		go svc.Watch(ctx, 5*time.Second)
	}
	serveObservability(ctx, cfg, log, m, health.NewChecker(version, checks...))

	p := &app.Processor{Cfg: cfg, Log: log, Client: client, Store: store, Registry: registry, Proc: proc, Metrics: m}
	log.Info("processor started", "shards", cfg.Processing.Workers, "queue_size", cfg.Processing.QueueSize,
		"max_deliver", cfg.Processing.MaxDeliver, "scripting", cfg.Scripting.Enabled)
	err = p.Run(ctx)
	log.Info("processor stopped")
	return err
}
