package telemetry

import (
	"context"
	"time"

	"github.com/alfscherer/infra-observer/internal/messaging"
)

// Consumer names a durable consumer whose backlog is exported.
type Consumer struct{ Stream, Durable string }

// WatchBacklog samples JetStream consumer state on an interval and exports it
// as observation_queue_depth. This is the number that answers the operational
// question "is collection outrunning processing?": the backlog waits in
// JetStream, durable and visible, rather than in memory.
func (m *Metrics) WatchBacklog(ctx context.Context, c *messaging.Client, every time.Duration, consumers ...Consumer) {
	sample := func() {
		for _, cs := range consumers {
			sctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			pending, ackPending, err := c.ConsumerBacklog(sctx, cs.Stream, cs.Durable)
			cancel()
			if err != nil {
				continue // the gauge keeps its last value; readiness reports NATS trouble
			}
			m.QueueDepth.WithLabelValues(cs.Durable).Set(float64(pending))
			m.QueueAckPending.WithLabelValues(cs.Durable).Set(float64(ackPending))
		}
	}
	sample()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sample()
		}
	}
}
