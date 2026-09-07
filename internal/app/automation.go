package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/pipeline"
	"github.com/alfscherer/infra-observer/internal/telemetry"
)

// Automation is the automation role: it consumes alert events, works through
// due requests on a timer and relays its own outbox.
type Automation struct {
	Cfg     config.Config
	Log     *slog.Logger
	Client  *messaging.Client
	Store   persistence.Store
	Engine  *automation.Engine
	Handler *automation.Worker
	Metrics *telemetry.Metrics // optional

	DurableSuffix string
	Worker        *messaging.Worker
	opts          messaging.WorkerOptions
}

// Build creates the message worker.
func (a *Automation) Build() {
	a.opts = WorkerOptions(a.Cfg, a.Log, a.Metrics, "automation", messaging.StreamEvents, "automation"+a.DurableSuffix, messaging.SubjectEventAlert, DeviceKey)
	a.Worker = a.Client.NewWorker(a.opts, func(ctx context.Context, m messaging.Message) error { return a.Handler.Handle(ctx, m.Data()) })
	if a.Metrics != nil {
		a.Metrics.AttachWorker("automation", a.Worker)
	}
}

// Run blocks until ctx ends or a component fails.
func (a *Automation) Run(ctx context.Context) error {
	if a.Worker == nil {
		a.Build()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := a.Client.WatchMaxDeliveries(ctx, a.opts.Stream, a.opts.Durable, ""); err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		relay := &pipeline.Relay{Store: a.Store, Publisher: a.Client, Log: a.Log}
		if a.Metrics != nil {
			relay.OnPublished = func(n int) { a.Metrics.OutboxPublished.Add(float64(n)) }
		}
		relay.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		t := time.NewTicker(a.Cfg.Automation.TickInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := a.Engine.Tick(ctx); err != nil && ctx.Err() == nil {
					a.Log.Warn("automation tick failed; will retry", "error", err)
				}
			}
		}
	}()
	if a.Metrics != nil {
		go a.Metrics.WatchBacklog(ctx, a.Client, 5*time.Second, telemetry.Consumer{Stream: a.opts.Stream, Durable: a.opts.Durable})
	}
	err := a.Worker.Run(ctx)
	cancel()
	wg.Wait()
	return err
}
