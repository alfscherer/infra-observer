package messaging

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/schema"
)

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func testOpts(durable string) WorkerOptions {
	return WorkerOptions{
		Name: "test", Stream: StreamTelemetryRaw, Durable: durable, FilterSubject: SubjectRawSNMP,
		Shards: 4, QueueSize: 4, MaxDeliver: 3, AckWait: 2 * time.Second, RetryDelay: 40 * time.Millisecond,
		MaxRetryDelay: 200 * time.Millisecond, ShutdownTimeout: 3 * time.Second, DeadLetterRetryDelay: 100 * time.Millisecond,
	}
}

// start runs a worker and returns it with a stop function that waits for Run.
func start(t *testing.T, c *Client, o WorkerOptions, h Handler) (*Worker, func()) {
	t.Helper()
	w := c.NewWorker(o, h)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(ctx); err != nil {
			t.Errorf("worker: %v", err)
		}
	}()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return w, stop
}

func pub(t *testing.T, c *Client, subject, id, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Publish(ctx, subject, id, []byte(body), map[string]string{HeaderCorrelationID: "corr-" + id}); err != nil {
		t.Fatal(err)
	}
}

func deadLetters(t *testing.T, c *Client) []DeadLetterRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := c.ListDeadLetters(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestWorkerAcksOnlyAfterSuccess(t *testing.T) {
	c, _ := newClient(t)
	var n atomic.Int32
	w, _ := start(t, c, testOpts("t-ack"), func(context.Context, Message) error { n.Add(1); return nil })
	for i := 0; i < 10; i++ {
		pub(t, c, SubjectRawSNMP, fmt.Sprint("m", i), "x")
	}
	eventually(t, 10*time.Second, "10 messages processed", func() bool { return w.Stats().Processed == 10 })
	time.Sleep(200 * time.Millisecond)
	if n.Load() != 10 {
		t.Fatalf("each message handled once, got %d", n.Load())
	}
	cons, _ := c.js.Consumer(context.Background(), StreamTelemetryRaw, "t-ack")
	info, _ := cons.Info(context.Background())
	if info.NumAckPending != 0 || info.NumPending != 0 || len(deadLetters(t, c)) != 0 {
		t.Fatalf("nothing may be left pending or dead-lettered: %+v", info)
	}
}

func TestTransientFailuresAreRetriedWithBackoffThenSucceed(t *testing.T) {
	c, _ := newClient(t)
	var mu sync.Mutex
	var attempts []uint64
	var stamps []time.Time
	w, _ := start(t, c, testOpts("t-retry"), func(_ context.Context, m Message) error {
		mu.Lock()
		defer mu.Unlock()
		attempts, stamps = append(attempts, m.Attempt()), append(stamps, time.Now())
		if len(attempts) < 3 {
			return domain.Errorf(domain.CategoryDependency, "database unavailable")
		}
		return nil
	})
	pub(t, c, SubjectRawSNMP, "m1", "payload")
	eventually(t, 10*time.Second, "success on the third attempt", func() bool { return w.Stats().Processed == 1 })
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(attempts) != "[1 2 3]" {
		t.Fatalf("attempts: %v", attempts)
	}
	if gap := stamps[1].Sub(stamps[0]); gap < 25*time.Millisecond {
		t.Fatalf("retries must back off, first gap was %v", gap)
	}
	if st := w.Stats(); st.Retried != 2 || st.DeadLettered != 0 || len(deadLetters(t, c)) != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestPermanentErrorsGoStraightToTheDeadLetterStream(t *testing.T) {
	c, _ := newClient(t)
	var calls atomic.Int32
	w, _ := start(t, c, testOpts("t-poison"), func(context.Context, Message) error {
		calls.Add(1)
		return domain.Errorf(domain.CategoryValidation, "observation has no metric")
	})
	pub(t, c, SubjectRawSNMP, "bad-1", `{"garbage":true}`)
	eventually(t, 10*time.Second, "dead letter", func() bool { return len(deadLetters(t, c)) == 1 })
	time.Sleep(400 * time.Millisecond) // a poison message must not come back
	if calls.Load() != 1 {
		t.Fatalf("invalid input must not be retried; handler ran %d times", calls.Load())
	}
	dl := deadLetters(t, c)[0]
	if string(dl.Payload) != `{"garbage":true}` || dl.Category != "validation" || dl.Reason != "poison" || dl.Consumer != "t-poison" ||
		dl.Attempts != 1 || dl.Subject != SubjectRawSNMP || dl.Stream != StreamTelemetryRaw || dl.Sequence == 0 ||
		!strings.Contains(dl.Error, "no metric") || dl.Headers[HeaderCorrelationID][0] != "corr-bad-1" || dl.FailedAt.IsZero() {
		t.Fatalf("the envelope must preserve the original message and explain the failure: %+v", dl)
	}
	if w.Stats().DeadLettered != 1 {
		t.Fatalf("%+v", w.Stats())
	}
}

func TestRetryExhaustionDeadLettersInsteadOfLoopingForever(t *testing.T) {
	c, _ := newClient(t)
	var calls atomic.Int32
	start(t, c, testOpts("t-exhaust"), func(context.Context, Message) error {
		calls.Add(1)
		return domain.Errorf(domain.CategoryTimeout, "still down")
	})
	pub(t, c, SubjectRawSNMP, "m1", "x")
	eventually(t, 10*time.Second, "dead letter after exhausting attempts", func() bool { return len(deadLetters(t, c)) == 1 })
	time.Sleep(500 * time.Millisecond)
	dl := deadLetters(t, c)[0]
	if calls.Load() != 3 || dl.Reason != "max_deliveries" || dl.Attempts != 3 || dl.Category != "timeout" {
		t.Fatalf("calls=%d %+v (no infinite retry loops)", calls.Load(), dl)
	}
}

func TestPanicsAndUntypedErrorsAreNotRetried(t *testing.T) {
	c, _ := newClient(t)
	var calls atomic.Int32
	w, _ := start(t, c, testOpts("t-panic"), func(_ context.Context, m Message) error {
		calls.Add(1)
		switch string(m.Data()) {
		case "panic":
			panic("nil map write")
		case "untyped":
			return errors.New("mystery")
		}
		return nil
	})
	pub(t, c, SubjectRawSNMP, "p1", "panic")
	pub(t, c, SubjectRawSNMP, "u1", "untyped")
	pub(t, c, SubjectRawSNMP, "ok", "fine")
	eventually(t, 10*time.Second, "both bad messages dead-lettered and the good one processed", func() bool {
		return len(deadLetters(t, c)) == 2 && w.Stats().Processed == 1
	})
	time.Sleep(300 * time.Millisecond)
	if calls.Load() != 3 || w.Stats().Panics != 1 {
		t.Fatalf("calls=%d stats=%+v: a handler that panics on a message will panic again, so it is dead-lettered at once", calls.Load(), w.Stats())
	}
}

func TestDeadLetterPublishFailureNeverLosesTheMessage(t *testing.T) {
	c, _ := newClient(t)
	o := testOpts("t-dlqfail")
	o.DeadLetterSubject = "no.stream.covers.this" // publishing the dead letter will fail
	var calls atomic.Int32
	w, _ := start(t, c, o, func(context.Context, Message) error {
		calls.Add(1)
		return domain.Errorf(domain.CategoryValidation, "poison")
	})
	pub(t, c, SubjectRawSNMP, "m1", "x")
	eventually(t, 15*time.Second, "the message to be offered again after the failed dead-lettering", func() bool { return calls.Load() >= 2 })
	if w.Stats().DLQFailures == 0 {
		t.Fatalf("%+v", w.Stats())
	}
}

func TestPerKeyOrderingWithBoundedParallelism(t *testing.T) {
	c, _ := newClient(t)
	o := testOpts("t-order")
	o.Shards, o.QueueSize = 3, 8
	o.KeyFunc = func(m Message) string { return strings.SplitN(string(m.Data()), ":", 2)[0] }
	var mu sync.Mutex
	last := map[string]int{}
	var running, peak atomic.Int32
	var bad atomic.Int32
	w, _ := start(t, c, o, func(_ context.Context, m Message) error {
		parts := strings.SplitN(string(m.Data()), ":", 2)
		var seq int
		fmt.Sscan(parts[1], &seq)
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(4 * time.Millisecond)
		mu.Lock()
		if seq != last[parts[0]]+1 {
			bad.Add(1)
		}
		last[parts[0]] = seq
		mu.Unlock()
		running.Add(-1)
		return nil
	})
	const keys, per = 12, 15
	for s := 1; s <= per; s++ { // interleave keys, ascending sequence per key
		for k := 0; k < keys; k++ {
			pub(t, c, SubjectRawSNMP, fmt.Sprintf("k%d-%d", k, s), fmt.Sprintf("device-%d:%d", k, s))
		}
	}
	eventually(t, 30*time.Second, "all messages", func() bool { return w.Stats().Processed == keys*per })
	if bad.Load() != 0 {
		t.Fatalf("%d messages were handled out of order for their key", bad.Load())
	}
	if peak.Load() > 3 || peak.Load() < 2 {
		t.Fatalf("concurrency must be bounded by the shard count (3) yet real parallelism must happen: peak=%d", peak.Load())
	}
}

func TestBackpressureBoundsWorkAndLeavesTheBacklogInJetStream(t *testing.T) {
	c, _ := newClient(t)
	o := testOpts("t-bp")
	o.Shards, o.QueueSize = 2, 2 // capacity 6
	o.AckWait = 30 * time.Second
	gate := make(chan struct{})
	var handled atomic.Int32
	w, _ := start(t, c, o, func(context.Context, Message) error { <-gate; handled.Add(1); return nil })
	const total = 80
	for i := 0; i < total; i++ {
		pub(t, c, SubjectRawSNMP, fmt.Sprint("m", i), fmt.Sprint("k", i))
	}
	time.Sleep(700 * time.Millisecond) // let the worker fetch as much as it is willing to
	st := w.Stats()
	if int(st.InFlight)+st.QueueDepth > st.Capacity {
		t.Fatalf("worker holds %d messages, more than its capacity %d", int(st.InFlight)+st.QueueDepth, st.Capacity)
	}
	if st.Received > uint64(st.Capacity)+2 {
		t.Fatalf("worker pulled %d messages although it can only hold %d: unbounded prefetch", st.Received, st.Capacity)
	}
	cons, _ := c.js.Consumer(context.Background(), StreamTelemetryRaw, "t-bp")
	info, _ := cons.Info(context.Background())
	if info.NumPending < total-uint64(st.Capacity)-5 || uint64(info.NumAckPending) > uint64(st.Capacity)*2 {
		t.Fatalf("the backlog must wait in JetStream (visible, durable), not in memory: %+v", info)
	}
	close(gate)
	eventually(t, 30*time.Second, "backlog drained once capacity returns", func() bool { return handled.Load() == total })
}

func TestConsumerRestartLosesNothing(t *testing.T) {
	c, _ := newClient(t)
	o := testOpts("t-restart")
	o.Shards, o.QueueSize = 2, 4
	var mu sync.Mutex
	done := map[string]int{}
	blockA := make(chan struct{})
	first, stopFirst := start(t, c, o, func(ctx context.Context, m Message) error {
		select {
		case <-blockA:
			return nil
		case <-ctx.Done():
			return ctx.Err() // the process is going away mid-message
		}
	})
	const total = 30
	for i := 0; i < total; i++ {
		pub(t, c, SubjectRawSNMP, fmt.Sprint("m", i), fmt.Sprint("m", i))
	}
	eventually(t, 10*time.Second, "worker A to be holding messages", func() bool { return first.Stats().InFlight > 0 })
	stopFirst() // "crash": handlers interrupted, buffered messages returned

	start(t, c, o, func(_ context.Context, m Message) error {
		mu.Lock()
		done[string(m.Data())]++
		mu.Unlock()
		return nil
	})
	eventually(t, 20*time.Second, "worker B to process every message", func() bool { mu.Lock(); defer mu.Unlock(); return len(done) == total })
	if len(deadLetters(t, c)) != 0 {
		t.Fatal("an interrupted handler is not poison")
	}
}

func TestSlowHandlerIsKeptAliveNotRedelivered(t *testing.T) {
	c, _ := newClient(t)
	o := testOpts("t-slow")
	o.AckWait = 600 * time.Millisecond
	var calls atomic.Int32
	w, _ := start(t, c, o, func(context.Context, Message) error { calls.Add(1); time.Sleep(2 * time.Second); return nil })
	pub(t, c, SubjectRawSNMP, "m1", "x")
	eventually(t, 15*time.Second, "slow message completed", func() bool { return w.Stats().Processed == 1 })
	if calls.Load() != 1 {
		t.Fatalf("a healthy but slow handler must not be redelivered (that would run it twice): %d calls", calls.Load())
	}
}

func TestMalformedTelemetryIsDeadLetteredWithItsOriginalBytes(t *testing.T) {
	c, _ := newClient(t)
	v := schema.NewValidator()
	var ok atomic.Int32
	w, _ := start(t, c, testOpts("t-malformed"), func(_ context.Context, m Message) error {
		if _, err := v.DecodeObservation(m.Data()); err != nil {
			return err
		}
		ok.Add(1)
		return nil
	})
	good := `{"observation_id":"o","correlation_id":"c","device_id":"d","source":"snmp","metric":"m.x","value":1,"observed_at":"2026-08-01T12:00:00Z"}`
	for i, body := range []string{"not json at all", `{"observation_id":"x"}`, good, `{"unknown_field":1}`} {
		pub(t, c, SubjectRawSNMP, fmt.Sprint("m", i), body)
	}
	eventually(t, 10*time.Second, "3 poison + 1 good", func() bool { return len(deadLetters(t, c)) == 3 && w.Stats().Processed == 1 })
	found := false
	for _, dl := range deadLetters(t, c) {
		if string(dl.Payload) == "not json at all" && dl.Category == "validation" {
			found = true
		}
	}
	if !found || ok.Load() != 1 {
		t.Fatal("garbage must be preserved byte for byte for inspection")
	}
}

func TestMaxDeliveriesAdvisoryRescuesAMessageAbandonedOnItsLastAttempt(t *testing.T) {
	c, _ := newClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A "worker" that takes every delivery and dies without settling: the case
	// the Worker itself cannot handle, because it never gets to report.
	cons, err := c.js.CreateOrUpdateConsumer(ctx, StreamTelemetryRaw, consumerCfg("t-abandon", 2, 300*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WatchMaxDeliveries(ctx, StreamTelemetryRaw, "t-abandon", ""); err != nil {
		t.Fatal(err)
	}
	pub(t, c, SubjectRawSNMP, "lost-1", "important")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && len(deadLetters(t, c)) == 0 {
		if batch, err := cons.Fetch(1, jetstreamMaxWait(500*time.Millisecond)); err == nil {
			for range batch.Messages() { // receive, never ack
			}
		}
	}
	dls := deadLetters(t, c)
	if len(dls) != 1 || string(dls[0].Payload) != "important" || dls[0].Reason != "max_deliveries" || dls[0].Consumer != "t-abandon" {
		t.Fatalf("a message whose deliveries ran out must surface in the dead-letter stream, not vanish: %+v", dls)
	}
}

func TestDeadLetterReplayAfterTheFixRunsTheMessageAgain(t *testing.T) {
	c, _ := newClient(t)
	var fixed atomic.Bool
	var mu sync.Mutex
	var seen []Message
	w, _ := start(t, c, testOpts("t-replay"), func(_ context.Context, m Message) error {
		if !fixed.Load() {
			return domain.Errorf(domain.CategoryValidation, "bug in the parser")
		}
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
		return nil
	})
	pub(t, c, SubjectRawSNMP, "m1", "the payload")
	eventually(t, 10*time.Second, "dead letter", func() bool { return len(deadLetters(t, c)) == 1 })
	rec := deadLetters(t, c)[0]

	fixed.Store(true) // "deploy the fix"
	if err := c.ReplayDeadLetter(context.Background(), rec.StreamSeq); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "replayed message processed", func() bool { return w.Stats().Processed == 1 })
	mu.Lock()
	defer mu.Unlock()
	if string(seen[0].Data()) != "the payload" || seen[0].Header().Get("X-Replay") != "true" || seen[0].Header().Get(HeaderCorrelationID) != "corr-m1" {
		t.Fatalf("payload and correlation id must survive the round trip: %s %v", seen[0].Data(), seen[0].Header())
	}
	if len(deadLetters(t, c)) != 0 {
		t.Fatal("a replayed dead letter is removed")
	}
	if err := c.ReplayDeadLetter(context.Background(), rec.StreamSeq); err == nil {
		t.Fatal("replaying twice must fail: it is gone")
	}
	if err := c.PurgeDeadLetters(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReplayStreamWindow(t *testing.T) {
	c, _ := newClient(t)
	ctx := context.Background()
	before := time.Now().Add(-time.Second)
	for i := 0; i < 5; i++ {
		pub(t, c, SubjectRawSNMP, fmt.Sprint("old", i), "x")
	}
	time.Sleep(150 * time.Millisecond)
	mid := time.Now()
	for i := 0; i < 3; i++ {
		pub(t, c, SubjectRawSNMP, fmt.Sprint("new", i), "y")
	}

	if n, err := c.ReplayStream(ctx, StreamTelemetryRaw, SubjectRawSNMP, before, time.Time{}, true); err != nil || n != 8 {
		t.Fatalf("dry run counts the whole window: %d %v", n, err)
	}
	if n, _ := c.ReplayStream(ctx, StreamTelemetryRaw, SubjectRawSNMP, mid, time.Time{}, true); n != 3 {
		t.Fatalf("since filter: %d", n)
	}
	if n, _ := c.ReplayStream(ctx, StreamTelemetryRaw, SubjectRawSNMP, before, mid, true); n != 5 {
		t.Fatalf("until filter: %d", n)
	}
	var got atomic.Int32
	var replayed atomic.Int32
	w, _ := start(t, c, testOpts("t-streamreplay"), func(_ context.Context, m Message) error {
		got.Add(1)
		if m.Header().Get("X-Replay") == "true" {
			replayed.Add(1)
		}
		return nil
	})
	eventually(t, 10*time.Second, "original 8 processed", func() bool { return w.Stats().Processed == 8 })
	if n, err := c.ReplayStream(ctx, StreamTelemetryRaw, SubjectRawSNMP, mid, time.Time{}, false); err != nil || n != 3 {
		t.Fatalf("%d %v", n, err)
	}
	eventually(t, 10*time.Second, "3 replayed messages delivered again", func() bool { return replayed.Load() == 3 })
}

func TestWorkerSurvivesNATSRestart(t *testing.T) {
	c, n := newClient(t)
	var mu sync.Mutex
	seen := map[string]bool{}
	start(t, c, testOpts("t-disconnect"), func(_ context.Context, m Message) error {
		mu.Lock()
		seen[string(m.Data())] = true
		mu.Unlock()
		return nil
	})
	for i := 0; i < 5; i++ {
		pub(t, c, SubjectRawSNMP, fmt.Sprint("a", i), fmt.Sprint("a", i))
	}
	eventually(t, 10*time.Second, "first batch", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 5 })

	n.Restart()
	eventually(t, 15*time.Second, "reconnect", c.Connected)
	deadline := time.Now().Add(15 * time.Second)
	for i := 0; i < 5; i++ {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := c.Publish(ctx, SubjectRawSNMP, fmt.Sprint("b", i), []byte(fmt.Sprint("b", i)), nil)
			cancel()
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("publish after restart: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	eventually(t, 20*time.Second, "the worker to resume consuming after the broker came back", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 10
	})
}

func TestShutdownLetsInFlightHandlersFinish(t *testing.T) {
	c, _ := newClient(t)
	o := testOpts("t-shutdown")
	var finished atomic.Bool
	started := make(chan struct{}, 1)
	w, stop := start(t, c, o, func(context.Context, Message) error {
		started <- struct{}{}
		time.Sleep(400 * time.Millisecond) // deliberately ignores ctx
		finished.Store(true)
		return nil
	})
	pub(t, c, SubjectRawSNMP, "m1", "x")
	<-started
	stop()
	if !finished.Load() || w.Stats().Processed != 1 {
		t.Fatalf("Run must wait for in-flight work: finished=%v stats=%+v", finished.Load(), w.Stats())
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	c, _ := newClient(t)
	w := c.NewWorker(WorkerOptions{RetryDelay: 100 * time.Millisecond, MaxRetryDelay: 1 * time.Second}, nil)
	within := func(d, target time.Duration) bool {
		return d >= time.Duration(float64(target)*0.79) && d <= time.Duration(float64(target)*1.21)
	}
	for attempt, want := range map[uint64]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond, 5: time.Second, 20: time.Second} {
		if got := w.backoff(attempt); !within(got, want) {
			t.Errorf("attempt %d: backoff %v, want ≈ %v ±20%%", attempt, got, want)
		}
	}
}

// helpers kept at the bottom so the tests above read top-down.

func consumerCfg(name string, maxDeliver int, ackWait time.Duration) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable: name, FilterSubject: SubjectRawSNMP, AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: maxDeliver, AckWait: ackWait,
	}
}

func jetstreamMaxWait(d time.Duration) jetstream.FetchOpt { return jetstream.FetchMaxWait(d) }
