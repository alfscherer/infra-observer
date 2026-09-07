// Package app assembles the long-running roles from their parts. The commands
// and the end-to-end tests both use it, so what the tests exercise is the wiring
// that production runs, not a re-implementation of it.
package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/alfscherer/infra-observer/internal/collector"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/pipeline"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

// WorkerOptions builds message-worker settings from configuration.
func WorkerOptions(cfg config.Config, log *slog.Logger, m *telemetry.Metrics, name, stream, durable, filter string, key func(messaging.Message) string) messaging.WorkerOptions {
	p := cfg.Processing
	o := messaging.WorkerOptions{
		Name: name, Stream: stream, Durable: durable, FilterSubject: filter,
		Shards: p.Workers, QueueSize: p.QueueSize, MaxDeliver: p.MaxDeliver, AckWait: p.AckWait, RetryDelay: p.RetryDelay,
		KeyFunc: key, Log: log,
	}
	if m != nil {
		o.OnOutcome = m.WorkerOutcome(name)
	}
	return o
}

// Processor is the processing role: stage A (raw -> normalized), stage B
// (normalized -> state, rules, persistence), the inventory consumer, the outbox
// relay and the stale-data sweeper.
type Processor struct {
	Cfg      config.Config
	Log      *slog.Logger
	Client   *messaging.Client
	Store    persistence.Store
	Registry *inventory.Registry
	Proc     *pipeline.Processor
	Metrics  *telemetry.Metrics // optional

	// DurableSuffix is appended to every consumer name. A different suffix gives
	// fresh consumers that read the streams from the beginning: that is how a
	// replay into an empty database is done.
	DurableSuffix string
	// KeyFunc routes messages to worker shards (default: by device).
	KeyFunc func(messaging.Message) string

	StageA, StageB, Inventory *messaging.Worker
	optsA, optsB, optsI       messaging.WorkerOptions
}

// DeviceKey routes a message to a shard by its device, so messages about one
// device are handled in order while different devices run in parallel.
func DeviceKey(m messaging.Message) string {
	var p struct {
		DeviceID string `json:"device_id"`
	}
	if jsonUnmarshal(m.Data(), &p) == nil && p.DeviceID != "" {
		return p.DeviceID
	}
	return m.Subject()
}

// Build creates the workers. It is separate from Run so callers can attach
// metrics to them first.
func (p *Processor) Build() {
	key := p.KeyFunc
	if key == nil {
		key = DeviceKey
	}
	p.optsA = WorkerOptions(p.Cfg, p.Log, p.Metrics, "normalizer", messaging.StreamTelemetryRaw, "normalizer"+p.DurableSuffix, messaging.SubjectRawPrefix+".>", key)
	p.StageA = p.Client.NewWorker(p.optsA, func(ctx context.Context, m messaging.Message) error {
		n, err := p.Proc.NormalizeMessage(ctx, m.Data())
		if err != nil {
			return err
		}
		if n.Dropped {
			return nil
		}
		return p.Client.Publish(ctx, messaging.SubjectNormalized, n.Observation.ObservationID, n.Payload, map[string]string{
			messaging.HeaderSchema: schema.ObservationV1, messaging.HeaderCorrelationID: n.Observation.CorrelationID,
		})
	})

	p.optsB = WorkerOptions(p.Cfg, p.Log, p.Metrics, "processor", messaging.StreamTelemetryNormalized, "processor"+p.DurableSuffix, messaging.SubjectNormalized, key)
	p.StageB = p.Client.NewWorker(p.optsB, func(ctx context.Context, m messaging.Message) error {
		obs, err := p.Proc.Validator.DecodeObservation(m.Data())
		if err != nil {
			return err
		}
		out, err := p.Proc.Process(ctx, obs)
		if err != nil {
			return err
		}
		if out.Duplicate {
			p.Log.Debug("duplicate delivery skipped", "observation_id", obs.ObservationID, "device_id", obs.DeviceID)
		}
		p.Registry.Touch(obs.DeviceID, obs.ObservedAt)
		return nil
	})

	p.optsI = WorkerOptions(p.Cfg, p.Log, p.Metrics, "inventory", messaging.StreamInventory, "inventory"+p.DurableSuffix, messaging.SubjectInventoryObserved, key)
	p.Inventory = p.Client.NewWorker(p.optsI, func(ctx context.Context, m messaging.Message) error {
		f, err := schema.Decode[collector.InventoryFact](m.Data(), schema.DefaultLimits().MaxMessageBytes)
		if err != nil {
			return err
		}
		if f.DeviceID == "" {
			return domain.Errorf(domain.CategoryValidation, "inventory fact without device_id")
		}
		return p.Store.UpdateDeviceAttributes(ctx, f.DeviceID, map[string]string{
			"observed.sys_name": f.SysName, "observed.sys_descr": f.SysDescr, "observed.sys_object_id": f.SysObjectID, "observed.profile": f.ProfileID,
		})
	})
	if p.Metrics != nil {
		p.Metrics.AttachWorker("normalizer", p.StageA)
		p.Metrics.AttachWorker("processor", p.StageB)
		p.Metrics.AttachWorker("inventory", p.Inventory)
	}
}

// Run starts everything and blocks until ctx ends. If a component stops
// unexpectedly the others are stopped too and its error is returned: a dead
// stage makes the process unhealthy, and the supervisor should restart it.
func (p *Processor) Run(ctx context.Context) error {
	if p.StageA == nil {
		p.Build()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				p.Log.Error("component stopped unexpectedly", "component", name, "error", err)
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				cancel()
			}
		}()
	}
	for _, w := range []struct {
		name string
		w    *messaging.Worker
		o    messaging.WorkerOptions
	}{{"normalizer", p.StageA, p.optsA}, {"processor", p.StageB, p.optsB}, {"inventory", p.Inventory, p.optsI}} {
		run(w.name, w.w.Run)
		// If a worker dies on a message's final delivery the server silently stops
		// redelivering it; this moves such messages to the dead-letter stream.
		if err := p.Client.WatchMaxDeliveries(ctx, w.o.Stream, w.o.Durable, ""); err != nil {
			return err
		}
	}
	relay := &pipeline.Relay{Store: p.Store, Publisher: p.Client, Log: p.Log}
	if p.Metrics != nil {
		relay.OnPublished = func(n int) { p.Metrics.OutboxPublished.Add(float64(n)) }
		go p.Metrics.WatchBacklog(ctx, p.Client, 5*time.Second,
			telemetry.Consumer{Stream: p.optsA.Stream, Durable: p.optsA.Durable},
			telemetry.Consumer{Stream: p.optsB.Stream, Durable: p.optsB.Durable},
			telemetry.Consumer{Stream: p.optsI.Stream, Durable: p.optsI.Durable})
	}
	run("outbox-relay", func(ctx context.Context) error { relay.Run(ctx); return nil })
	run("sweeper", func(ctx context.Context) error { p.sweepLoop(ctx); return nil })

	<-ctx.Done()
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return firstErr
}

func (p *Processor) sweepLoop(ctx context.Context) {
	t := time.NewTicker(p.Cfg.Processing.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			n, err := p.Proc.Sweep(ctx, now, p.Registry.List())
			if err != nil && ctx.Err() == nil {
				p.Log.Warn("stale-data sweep failed; will retry next interval", "error", err)
			} else if n > 0 {
				p.Log.Info("stale-data sweep recorded failed samples", "samples", n)
			}
		}
	}
}
