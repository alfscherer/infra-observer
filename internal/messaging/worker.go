package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/schema"
)

// WorkerOptions configure a Worker.
type WorkerOptions struct {
	// Name identifies the worker in logs and dead letters.
	Name          string
	Stream        string
	Durable       string // consumers sharing a durable name form a consumer group
	FilterSubject string

	// Shards is the number of concurrent handlers. A message is routed to a
	// shard by KeyFunc, so messages with the same key are handled one at a time
	// and in delivery order; messages with different keys run in parallel.
	Shards int
	// QueueSize bounds the messages buffered per shard. Together with Shards it
	// bounds how much work the worker holds: shards * (queue + 1). When the
	// buffers are full the worker stops fetching, and the backlog stays in
	// JetStream where it is visible and durable.
	QueueSize int

	MaxDeliver    int           // attempts before a message is dead-lettered
	AckWait       time.Duration // redelivery deadline; refreshed while a handler runs
	RetryDelay    time.Duration // first retry delay; doubles per attempt
	MaxRetryDelay time.Duration // cap on the retry delay (default 1m)

	// KeyFunc picks the ordering key (default: the subject).
	KeyFunc func(Message) string
	// DeadLetterSubject overrides system.deadletter (tests).
	DeadLetterSubject string
	// ShutdownTimeout bounds how long Run waits for in-flight handlers (default 30s).
	ShutdownTimeout time.Duration
	// DeadLetterRetryDelay is how long a message waits before another attempt
	// to dead-letter it when the dead-letter publish itself failed (default 5s).
	DeadLetterRetryDelay time.Duration

	Log *slog.Logger
	// OnOutcome, if set, is called after every message with its outcome
	// ("processed", "retry", "dead_letter", "dlq_failed") and how long it took.
	OnOutcome func(outcome string, subject string, d time.Duration)
}

// WorkerStats are cumulative counters and gauges, safe to read at any time.
type WorkerStats struct {
	Received     uint64
	Processed    uint64
	Retried      uint64
	DeadLettered uint64
	DLQFailures  uint64
	Panics       uint64
	InFlight     int64
	QueueDepth   int
	Capacity     int // shards * (queue + 1): the most work the worker will hold
}

// Worker is a durable JetStream consumer with bounded, ordered concurrency,
// category-driven retry, and dead-lettering.
//
// Delivery is at-least-once: a message is acknowledged only after its handler
// returns nil, so a crash mid-handler means redelivery. Handlers must be
// idempotent; this package does not pretend otherwise.
type Worker struct {
	c *Client
	o WorkerOptions
	h Handler

	shards []chan Message
	log    *slog.Logger

	received, processed, retried, dead, dlqFail, panics atomic.Uint64
	inflight                                            atomic.Int64
}

// NewWorker prepares a worker; call Run to start it.
func (c *Client) NewWorker(o WorkerOptions, h Handler) *Worker {
	if o.Shards < 1 {
		o.Shards = 1
	}
	if o.QueueSize < 1 {
		o.QueueSize = 1
	}
	if o.MaxDeliver < 1 {
		o.MaxDeliver = 5
	}
	if o.AckWait <= 0 {
		o.AckWait = 30 * time.Second
	}
	if o.RetryDelay <= 0 {
		o.RetryDelay = time.Second
	}
	if o.MaxRetryDelay <= 0 {
		o.MaxRetryDelay = time.Minute
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 30 * time.Second
	}
	if o.DeadLetterRetryDelay <= 0 {
		o.DeadLetterRetryDelay = 5 * time.Second
	}
	if o.DeadLetterSubject == "" {
		o.DeadLetterSubject = SubjectDeadLetter
	}
	if o.KeyFunc == nil {
		o.KeyFunc = func(m Message) string { return m.Subject() }
	}
	log := o.Log
	if log == nil {
		log = c.log
	}
	w := &Worker{c: c, o: o, h: h, log: log.With("component", "worker", "worker", o.Name, "consumer", o.Durable)}
	w.shards = make([]chan Message, o.Shards)
	for i := range w.shards {
		w.shards[i] = make(chan Message, o.QueueSize)
	}
	return w
}

// Stats returns the current counters and gauges.
func (w *Worker) Stats() WorkerStats {
	depth := 0
	for _, s := range w.shards {
		depth += len(s)
	}
	return WorkerStats{
		Received: w.received.Load(), Processed: w.processed.Load(), Retried: w.retried.Load(), DeadLettered: w.dead.Load(),
		DLQFailures: w.dlqFail.Load(), Panics: w.panics.Load(), InFlight: w.inflight.Load(), QueueDepth: depth,
		Capacity: w.o.Shards * (w.o.QueueSize + 1),
	}
}

func (w *Worker) shardFor(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(w.shards)))
}

// Run consumes until ctx is cancelled, then stops fetching, waits for
// in-flight handlers, and returns. Messages still buffered are negatively
// acknowledged so another worker takes them immediately.
func (w *Worker) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // also stops the shard goroutines if startup fails below
	capacity := w.o.Shards * (w.o.QueueSize + 1)
	cons, err := w.c.js.CreateOrUpdateConsumer(ctx, w.o.Stream, jetstream.ConsumerConfig{
		Durable: w.o.Durable, FilterSubject: w.o.FilterSubject, AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: w.o.AckWait, MaxDeliver: w.o.MaxDeliver, MaxAckPending: capacity * 2,
	})
	if err != nil {
		return domain.Wrap(domain.CategoryDependency, "create consumer "+w.o.Durable, err)
	}

	var wg sync.WaitGroup
	for _, ch := range w.shards {
		wg.Add(1)
		go func(ch chan Message) {
			defer wg.Done()
			for {
				select {
				case m := <-ch:
					w.process(ctx, m)
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}

	cc, err := cons.Consume(func(jm jetstream.Msg) {
		m := Message{msg: jm}
		w.received.Add(1)
		// Blocking here is the backpressure: while every shard buffer is full
		// this callback does not return, so the client stops pulling more.
		select {
		case w.shards[w.shardFor(w.o.KeyFunc(m))] <- m:
		case <-ctx.Done():
			_ = m.Nak(0)
		}
	}, jetstream.PullMaxMessages(capacity), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		w.log.Warn("consumer error", "error", err)
	}))
	if err != nil {
		// no messages will ever arrive; let the shard goroutines observe cancellation
		return domain.Wrap(domain.CategoryDependency, "start consumer "+w.o.Durable, err)
	}

	<-ctx.Done()
	cc.Stop()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.o.ShutdownTimeout):
		w.log.Warn("shutdown timed out with handlers still running; their messages will be redelivered")
	}
	// Whatever was buffered but never started goes straight back to the server
	// so another worker can take it immediately instead of after AckWait.
	for _, ch := range w.shards {
		for drained := false; !drained; {
			select {
			case m := <-ch:
				_ = m.Nak(0)
			default:
				drained = true
			}
		}
	}
	return nil
}

func (w *Worker) backoff(attempt uint64) time.Duration {
	d := w.o.RetryDelay
	for i := uint64(1); i < attempt && d < w.o.MaxRetryDelay; i++ {
		d *= 2
	}
	if d > w.o.MaxRetryDelay {
		d = w.o.MaxRetryDelay
	}
	jitter := 0.8 + 0.4*rand.Float64() // ±20%: retries of a burst must not re-arrive as a burst
	return time.Duration(float64(d) * jitter)
}

// process runs the handler for one message and settles it.
func (w *Worker) process(ctx context.Context, m Message) {
	start := time.Now()
	if ctx.Err() != nil { // shutting down: hand it back without running it
		_ = m.Nak(0)
		return
	}
	w.inflight.Add(1)
	defer w.inflight.Add(-1)

	attempt := m.Attempt()
	stopBeat := w.heartbeat(m)
	err := w.safeHandle(ctx, m)
	stopBeat()

	report := func(outcome string) {
		if w.o.OnOutcome != nil {
			w.o.OnOutcome(outcome, m.Subject(), time.Since(start))
		}
	}
	switch {
	case err == nil:
		if aerr := m.Ack(); aerr != nil {
			w.log.Warn("ack failed; message may be redelivered", "subject", m.Subject(), "error", aerr)
		}
		w.processed.Add(1)
		report("processed")

	case ctx.Err() != nil:
		_ = m.Nak(0) // shutdown interrupted the handler: redeliver, do not count an attempt against it as poison
		report("retry")

	case domain.IsRetryable(err) && attempt < uint64(w.o.MaxDeliver):
		delay := w.backoff(attempt)
		w.log.Warn("handler failed; will retry", "subject", m.Subject(), "attempt", attempt, "max_deliver", w.o.MaxDeliver,
			"category", domain.CategoryOf(err), "delay", delay, "error", err)
		_ = m.Nak(delay)
		w.retried.Add(1)
		report("retry")

	default:
		reason := "poison"
		if domain.IsRetryable(err) {
			reason = "max_deliveries"
		}
		if derr := w.deadLetter(ctx, m, err, reason, attempt); derr != nil {
			// Losing the message is worse than delaying it: leave it in the
			// stream and try again later.
			w.dlqFail.Add(1)
			w.log.Error("dead-letter publish failed; message stays queued for another attempt", "subject", m.Subject(), "error", derr)
			_ = m.Nak(w.backoff(attempt) + w.o.DeadLetterRetryDelay)
			report("dlq_failed")
			return
		}
		_ = m.Term()
		w.dead.Add(1)
		report("dead_letter")
	}
}

// safeHandle runs the handler and turns a panic into a permanent error: a
// handler that panics on a message will panic on it again, so retrying it
// would only loop.
func (w *Worker) safeHandle(ctx context.Context, m Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.panics.Add(1)
			w.log.Error("handler panicked", "subject", m.Subject(), "panic", fmt.Sprint(r), "stack", string(debug.Stack()))
			err = domain.Errorf(domain.CategoryPermanent, "handler panic: %v", r)
		}
	}()
	return w.h(ctx, m)
}

// heartbeat tells the server the message is still being worked on, so a slow
// but healthy handler is not redelivered (and run twice) when it exceeds AckWait.
func (w *Worker) heartbeat(m Message) (stop func()) {
	t := time.NewTicker(w.o.AckWait / 3)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-t.C:
				_ = m.InProgress()
			case <-done:
				return
			}
		}
	}()
	return func() { t.Stop(); close(done) }
}

// DeadLetter is the envelope published to system.deadletter. It carries the
// original payload untouched, so a fixed system can replay it.
type DeadLetter struct {
	ID       string              `json:"id"`
	Consumer string              `json:"consumer"`
	Stream   string              `json:"stream"`
	Sequence uint64              `json:"sequence"`
	Subject  string              `json:"subject"`
	Headers  map[string][]string `json:"headers,omitempty"`
	Payload  []byte              `json:"payload"`
	Error    string              `json:"error"`
	Category string              `json:"category"`
	Reason   string              `json:"reason"` // poison, max_deliveries
	Attempts uint64              `json:"attempts"`
	FailedAt time.Time           `json:"failed_at"`
}

func (w *Worker) deadLetter(ctx context.Context, m Message, cause error, reason string, attempts uint64) error {
	dl := DeadLetter{
		Consumer: w.o.Durable, Stream: w.o.Stream, Sequence: m.StreamSeq(), Subject: m.Subject(),
		Headers: map[string][]string(m.Header()), Payload: m.Data(), Error: cause.Error(),
		Category: string(domain.CategoryOf(cause)), Reason: reason, Attempts: attempts, FailedAt: time.Now().UTC(),
	}
	dl.ID = domain.StableID("dlq", dl.Consumer, dl.Stream, fmt.Sprint(dl.Sequence))
	payload, err := json.Marshal(dl)
	if err != nil {
		return err
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	// The message id is derived from (consumer, stream, sequence), so a
	// redelivery that fails again cannot fill the dead-letter stream with copies.
	if err := w.c.Publish(pctx, w.o.DeadLetterSubject, dl.ID, payload,
		map[string]string{HeaderSchema: schema.DeadLetterV1, HeaderCorrelationID: m.Header().Get(HeaderCorrelationID)}); err != nil {
		return err
	}
	w.log.Warn("message dead-lettered", "subject", m.Subject(), "stream_seq", dl.Sequence, "reason", reason,
		"category", dl.Category, "attempts", attempts, "error", cause, "dead_letter_id", dl.ID)
	return nil
}
