package main

import (
	"context"
	"flag"
	"time"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/automation/adapters"
	"github.com/alfscherer/infra-observer/internal/collector/snmp"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/health"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/pipeline"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/secrets"
	"github.com/alfscherer/infra-observer/internal/sim"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

// cmdAutomationWorker runs the automation subsystem: it consumes alert events,
// lets policies and integrations react, and works through due requests.
func cmdAutomationWorker(args []string) error {
	fs := flag.NewFlagSet("automation-worker", flag.ContinueOnError)
	path := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log := newLogger(cfg.Logging, "automation")
	ctx, stop := signalContext()
	defer stop()

	devices, err := inventory.LoadFile(cfg.Inventory.File)
	if err != nil {
		return err
	}
	registry := inventory.NewRegistry(devices)
	policies, err := automation.LoadFile(cfg.Automation.PoliciesFile)
	if err != nil {
		return err
	}
	m := telemetry.New()
	store, err := connectDatabase(ctx, cfg, log, false)
	if err != nil {
		return err
	}
	defer store.Close()
	store.OnError = m.DatabaseError
	client, err := connectNATS(ctx, cfg, "automation-worker", log)
	if err != nil {
		return err
	}
	defer client.Close()

	checks := []health.Check{postgresCheck(store), natsCheck(client)}
	resolver := secrets.EnvFile{Dir: cfg.Secrets.Dir}
	validator := schema.NewValidator()

	// Scripts: automation proposers and integrations.
	var ext *scripting.Extensions
	var runner scripting.IntegrationRunner
	if cfg.Scripting.Enabled {
		svc, rep := scripting.New(cfg.Scripting, cfg.Scripts, []runtime.Installer{api.Installer(api.Deps{
			Inventory: registry, State: store, Endpoints: cfg.Scripting.Endpoints, Secrets: resolver, Log: log,
		})}, log)
		defer svc.Close()
		log.Info("scripting enabled", "loaded", rep.Loaded, "failed", rep.Failed, "disabled", rep.Disabled)
		ext = &scripting.Extensions{Svc: svc, Validator: validator, Log: log}
		m.AttachScripting(svc)
		checks = append(checks, scriptingCheck(svc))
		runner = scripting.IntegrationRunner{Ext: ext, Log: log, Persist: func(ctx context.Context, ev domain.Event) error {
			return pipeline.PersistEvent(ctx, store, ev)
		}}
		go svc.Watch(ctx, 5*time.Second)
	}

	// Adapters. Mutations go to the simulator through its control channel; reads
	// are real SNMP. A production deployment would supply real controllers.
	sn := cfg.Collectors.SNMP
	reader := &adapters.Reader{Dialer: snmp.GoSNMPDialer{Timeout: sn.Timeout, Retries: sn.Retries, MaxRepetitions: uint32(sn.MaxRepetitions)}, Secrets: resolver}
	generic := &adapters.Generic{
		Endpoints: cfg.Scripting.Endpoints, Secrets: resolver,
		Record: func(ctx context.Context, ev domain.Event) error { return pipeline.PersistEvent(ctx, store, ev) },
	}
	if ext != nil {
		generic.Invoke = func(ctx context.Context, script string, dev domain.Device, a automation.Action) (string, error) {
			return runner.Invoke(ctx, script, dev, a.RequestID, a.CorrelationID, a.Params)
		}
	}
	engine := &automation.Engine{
		Policies: policies, Store: store, Devices: registry, GlobalDryRun: cfg.Automation.DefaultDryRun,
		Adapters: adapters.NewSet(sim.DeviceController{C: sim.NATSController{Conn: client.Conn()}}, reader, generic), Log: log,
		OnRequest: m.AutomationRequest, OnResult: m.AutomationResult,
	}
	if ext != nil {
		engine.Proposer = ext
	}
	handler := &automation.Worker{Engine: engine, Devices: registry, Log: log}
	if ext != nil {
		handler.Integrations = runner
	}

	opts := workerOptions(cfg, log, m, "automation", messaging.StreamEvents, "automation", messaging.SubjectEventAlert)
	worker := client.NewWorker(opts, func(ctx context.Context, msg messaging.Message) error { return handler.Handle(ctx, msg.Data()) })
	m.AttachWorker("automation", worker)
	serveObservability(ctx, cfg, log, m, health.NewChecker(version, checks...))
	go m.WatchBacklog(ctx, client, 5*time.Second, telemetry.Consumer{Stream: opts.Stream, Durable: opts.Durable})

	go func() {
		relay := &pipeline.Relay{Store: store, Publisher: client, Log: log, OnPublished: func(n int) { m.OutboxPublished.Add(float64(n)) }}
		relay.Run(ctx)
	}()
	go func() {
		t := time.NewTicker(cfg.Automation.TickInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := engine.Tick(ctx); err != nil && ctx.Err() == nil {
					log.Warn("automation tick failed; will retry", "error", err)
				}
			}
		}
	}()
	if err := client.WatchMaxDeliveries(ctx, messaging.StreamEvents, "automation", ""); err != nil {
		return err
	}
	log.Info("automation worker started", "default_dry_run", cfg.Automation.DefaultDryRun, "policies", len(policies.All()))
	if cfg.Automation.DefaultDryRun {
		log.Info("automation is in DRY-RUN mode: no action will change any device")
	} else {
		log.Warn("automation is LIVE: policies that set dry_run: false will mutate allowlisted devices")
	}
	err = worker.Run(ctx)
	log.Info("automation worker stopped")
	return err
}
