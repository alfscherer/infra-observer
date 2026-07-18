package messaging

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/testutil"
)

func newClient(t *testing.T) (*Client, *testutil.NATS) {
	t.Helper()
	n := testutil.StartNATS(t)
	c, err := Connect(n.URL(), "test", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	return c, n
}

func TestEnsureStreamsIsIdempotent(t *testing.T) {
	c, _ := newClient(t)
	if err := c.EnsureStreams(context.Background()); err != nil {
		t.Fatalf("second EnsureStreams: %v", err)
	}
	for _, spec := range StreamSpecs() {
		if _, err := c.js.Stream(context.Background(), spec.Name); err != nil {
			t.Errorf("stream %s missing: %v", spec.Name, err)
		}
	}
}

func TestPublishDeduplicatesByMessageID(t *testing.T) {
	c, _ := newClient(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := c.Publish(ctx, SubjectRawSNMP, "obs-1", []byte(`{}`), nil); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := c.js.Stream(ctx, StreamTelemetryRaw)
	info, _ := s.Info(ctx)
	if info.State.Msgs != 1 {
		t.Fatalf("publisher retries with the same id must collapse to 1 message, got %d", info.State.Msgs)
	}
}

func TestPublishToUnknownSubjectFails(t *testing.T) {
	c, _ := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Publish(ctx, "nobody.listens", "", []byte("x"), nil); err == nil {
		t.Fatal("publishing to a subject without a stream must fail")
	}
}

func TestConsumeAcksOnlyAfterSuccessAndRedeliversOnError(t *testing.T) {
	c, _ := newClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts atomic.Int32
	done := make(chan struct{})
	go func() {
		_ = c.Consume(ctx, ConsumerSpec{
			Stream: StreamTelemetryRaw, Durable: "t-redeliver", FilterSubject: SubjectRawSNMP,
			AckWait: 5 * time.Second, MaxDeliver: 5, MaxAckPending: 10,
		}, func(_ context.Context, m Message) error {
			if attempts.Add(1) < 3 {
				return domain.Errorf(domain.CategoryTransient, "not yet")
			}
			close(done)
			return nil
		})
	}()
	if err := c.Publish(ctx, SubjectRawSNMP, "m1", []byte("hello"), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("message not redelivered until success; attempts=%d", attempts.Load())
	}
	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", attempts.Load())
	}
}

func TestConsumerGroupSharesWork(t *testing.T) {
	c, _ := newClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	seen := map[string]int{}
	var total atomic.Int32
	handler := func(_ context.Context, m Message) error {
		mu.Lock()
		seen[string(m.Data())]++
		mu.Unlock()
		total.Add(1)
		return nil
	}
	spec := ConsumerSpec{Stream: StreamTelemetryNormalized, Durable: "t-group", FilterSubject: SubjectNormalized, AckWait: 5 * time.Second, MaxDeliver: 3, MaxAckPending: 10}
	for i := 0; i < 2; i++ {
		go func() { _ = c.Consume(ctx, spec, handler) }()
	}
	for i := 0; i < 20; i++ {
		if err := c.Publish(ctx, SubjectNormalized, "", []byte{byte('a' + i)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for total.Load() < 20 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 20 || total.Load() != 20 {
		t.Fatalf("expected each of 20 messages exactly once across the group, got %d distinct / %d total", len(seen), total.Load())
	}
}

func TestReconnectAfterServerRestart(t *testing.T) {
	c, n := newClient(t)
	ctx := context.Background()
	n.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for c.Connected() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if c.Connected() {
		t.Fatal("client should notice the outage")
	}
	if err := c.Ping(ctx); domain.CategoryOf(err) != domain.CategoryDependency {
		t.Fatalf("ping during outage: %v", err)
	}
	n.Restart()
	deadline = time.Now().Add(10 * time.Second)
	for !c.Connected() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !c.Connected() {
		t.Fatal("client did not reconnect")
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Publish(pctx, SubjectRawSNMP, "after-restart", []byte("x"), nil); err != nil {
		t.Fatalf("publish after reconnect: %v", err)
	}
}
