package pipeline

import (
	"context"
	"log/slog"
	"time"

	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/persistence"
)

// BatchPublisher is what the relay needs from the messaging client.
type BatchPublisher interface {
	PublishBatch(ctx context.Context, msgs []messaging.OutMsg) error
}

// Relay moves outbox rows to NATS. It is the second half of the transactional
// outbox: events are committed with the state change, then published here, at
// least once. A crash between publish and marking published re-sends the batch;
// the Nats-Msg-Id header lets JetStream drop the copy inside its duplicate
// window, and consumers are idempotent for anything beyond that.
type Relay struct {
	Store     persistence.Store
	Publisher BatchPublisher
	BatchSize int
	Interval  time.Duration
	Log       *slog.Logger
	// OnPublished, if set, is told how many messages each successful drain sent.
	OnPublished func(n int)
}

// Drain publishes pending messages until the outbox is empty or an error occurs.
func (r *Relay) Drain(ctx context.Context) (int, error) {
	total := 0
	size := r.BatchSize
	if size <= 0 {
		size = 100
	}
	for {
		n, err := r.Store.DrainOutbox(ctx, size, func(ctx context.Context, batch []persistence.OutboxMessage) error {
			msgs := make([]messaging.OutMsg, len(batch))
			for i, m := range batch {
				msgs[i] = messaging.OutMsg{Subject: m.Subject, MsgID: m.MsgID, Payload: m.Payload, Headers: m.Headers}
			}
			return r.Publisher.PublishBatch(ctx, msgs)
		})
		total += n
		if n > 0 && r.OnPublished != nil {
			r.OnPublished(n)
		}
		if err != nil || n < size {
			return total, err
		}
	}
}

// Run drains on an interval until ctx ends. Errors are logged and retried on
// the next tick: an unreachable NATS or database delays events, never loses them.
func (r *Relay) Run(ctx context.Context) {
	log := r.Log
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "outbox-relay")
	interval := r.Interval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := r.Drain(ctx); err != nil && ctx.Err() == nil {
				log.Warn("outbox drain failed; will retry", "error", err)
			}
		}
	}
}
