package main

import (
	"context"
	"flag"
	"log/slog"
	"sync"
	"time"

	"github.com/alfscherer/infra-observer/internal/collector"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/enrich"
	"github.com/alfscherer/infra-observer/internal/health"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/messaging"
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
// plus the outbox relay, the stale-data sweeper and the inventory consumer.
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

	var wg sync.WaitGroup
	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				log.Error("component stopped unexpectedly", "component", name, "error", err)
				stop() // a dead stage makes the process unhealthy; exit and let the supervisor restart it
			}
		}()
	}

	// Stage A: raw -> normalized.
	optsA := workerOptions(cfg, log, m, "normalizer", messaging.StreamTelemetryRaw, "normalizer", messaging.SubjectRawPrefix+".>")
	stageA := client.NewWorker(optsA, func(ctx context.Context, m messaging.Message) error {
		n, err := proc.NormalizeMessage(ctx, m.Data())
		if err != nil {
			return err
		}
		if n.Dropped {
			return nil
		}
		return client.Publish(ctx, messaging.SubjectNormalized, n.Observation.ObservationID, n.Payload, map[string]string{
			messaging.HeaderSchema: schema.ObservationV1, messaging.HeaderCorrelationID: n.Observation.CorrelationID,
		})
	})
	// Stage B: normalized -> state, rules, persistence.
	optsB := workerOptions(cfg, log, m, "processor", messaging.StreamTelemetryNormalized, "processor", messaging.SubjectNormalized)
	stageB := client.NewWorker(optsB, func(ctx context.Context, m messaging.Message) error {
		obs, err := proc.Validator.DecodeObservation(m.Data())
		if err != nil {
			return err
		}
		out, err := proc.Process(ctx, obs)
		if err != nil {
			return err
		}
		if out.Duplicate {
			log.Debug("duplicate delivery skipped", "observation_id", obs.ObservationID, "device_id", obs.DeviceID)
		}
		registry.Touch(obs.DeviceID, obs.ObservedAt)
		return nil
	})
	// Inventory facts (sysName, sysDescr...) observed by collectors.
	optsI := workerOptions(cfg, log, m, "inventory", messaging.StreamInventory, "inventory", messaging.SubjectInventoryObserved)
	inv := client.NewWorker(optsI, func(ctx context.Context, m messaging.Message) error {
		f, err := schema.Decode[collector.InventoryFact](m.Data(), schema.DefaultLimits().MaxMessageBytes)
		if err != nil {
			return err
		}
		if f.DeviceID == "" {
			return domain.Errorf(domain.CategoryValidation, "inventory fact without device_id")
		}
		return store.UpdateDeviceAttributes(ctx, f.DeviceID, map[string]string{
			"observed.sys_name": f.SysName, "observed.sys_descr": f.SysDescr, "observed.sys_object_id": f.SysObjectID, "observed.profile": f.ProfileID,
		})
	})
	for _, w := range []struct {
		name string
		w    *messaging.Worker
		opts messaging.WorkerOptions
	}{{"normalizer", stageA, optsA}, {"processor", stageB, optsB}, {"inventory", inv, optsI}} {
		run(w.name, w.w.Run)
		// If a worker dies on a message's final delivery the server stops
		// redelivering it silently; this moves such messages to the dead-letter stream.
		if err := client.WatchMaxDeliveries(ctx, w.opts.Stream, w.opts.Durable, ""); err != nil {
			return err
		}
	}

	m.AttachWorker("normalizer", stageA)
	m.AttachWorker("processor", stageB)
	m.AttachWorker("inventory", inv)
	go m.WatchBacklog(ctx, client, 5*time.Second,
		telemetry.Consumer{Stream: optsA.Stream, Durable: optsA.Durable},
		telemetry.Consumer{Stream: optsB.Stream, Durable: optsB.Durable},
		telemetry.Consumer{Stream: optsI.Stream, Durable: optsI.Durable})
	relay := &pipeline.Relay{Store: store, Publisher: client, Log: log, OnPublished: func(n int) { m.OutboxPublished.Add(float64(n)) }}
	run("outbox-relay", func(ctx context.Context) error { relay.Run(ctx); return nil })
	run("sweeper", func(ctx context.Context) error { sweepLoop(ctx, cfg, proc, registry, log); return nil })

	log.Info("processor started", "shards", cfg.Processing.Workers, "queue_size", cfg.Processing.QueueSize,
		"max_deliver", cfg.Processing.MaxDeliver, "scripting", cfg.Scripting.Enabled)
	<-ctx.Done()
	wg.Wait()
	log.Info("processor stopped")
	return nil
}

func sweepLoop(ctx context.Context, cfg config.Config, proc *pipeline.Processor, reg inventory.Reader, log *slog.Logger) {
	t := time.NewTicker(cfg.Processing.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			n, err := proc.Sweep(ctx, now, reg.List())
			if err != nil && ctx.Err() == nil {
				log.Warn("stale-data sweep failed; will retry next interval", "error", err)
			} else if n > 0 {
				log.Info("stale-data sweep recorded failed samples", "samples", n)
			}
		}
	}
}
