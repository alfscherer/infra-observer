// Package messaging is a thin layer over nats.go and JetStream.
//
// It deliberately does not hide NATS behind a generic event-bus interface:
// subjects, streams, acknowledgements and redelivery are the architecture and
// the code says so. What it adds is provisioning (streams are declared in one
// place), publisher retry with categorised errors, and connection health.
package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// Client owns one NATS connection and its JetStream context.
type Client struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	log *slog.Logger
}

// Connect dials NATS. The connection reconnects forever: a NATS restart makes
// the process unready, not dead, and it heals without operator action.
func Connect(url, name string, log *slog.Logger) (*Client, error) {
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "messaging")
	nc, err := nats.Connect(url,
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
		nats.RetryOnFailedConnect(false),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { log.Warn("nats disconnected", "error", err) }),
		nats.ReconnectHandler(func(c *nats.Conn) { log.Info("nats reconnected", "url", c.ConnectedUrl()) }),
		nats.ClosedHandler(func(*nats.Conn) { log.Info("nats connection closed") }),
	)
	if err != nil {
		return nil, domain.Wrap(domain.CategoryDependency, "connect nats", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, domain.Wrap(domain.CategoryDependency, "init jetstream", err)
	}
	return &Client{nc: nc, js: js, log: log}, nil
}

// Conn exposes the raw connection for core-NATS subscriptions (sim.control).
func (c *Client) Conn() *nats.Conn { return c.nc }

// JetStream exposes the JetStream context for consumers built in this module.
func (c *Client) JetStream() jetstream.JetStream { return c.js }

// Connected reports whether the connection is currently usable.
func (c *Client) Connected() bool { return c.nc.IsConnected() }

// Close drains and closes the connection so in-flight acks are flushed.
func (c *Client) Close() {
	if err := c.nc.Drain(); err != nil {
		c.nc.Close()
	}
}

// Ping verifies the round trip to the server; used by readiness checks.
func (c *Client) Ping(ctx context.Context) error {
	if !c.nc.IsConnected() {
		return domain.Errorf(domain.CategoryDependency, "nats not connected")
	}
	if err := c.nc.FlushWithContext(ctx); err != nil {
		return domain.Wrap(domain.CategoryDependency, "nats flush", err)
	}
	return nil
}

// StreamSpecs are the streams the platform needs. Retention is time-bounded
// everywhere: JetStream is the replay window and buffer, PostgreSQL is the
// system of record.
func StreamSpecs() []jetstream.StreamConfig {
	day := 24 * time.Hour
	return []jetstream.StreamConfig{
		{
			Name: StreamTelemetryRaw, Subjects: []string{SubjectRawPrefix + ".>"},
			Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage,
			MaxAge: 2 * day, MaxBytes: 1 << 30, Discard: jetstream.DiscardOld,
			Duplicates:  2 * time.Minute,
			Description: "Source-specific observations exactly as collected; replay after normalization changes.",
		},
		{
			Name: StreamTelemetryNormalized, Subjects: []string{SubjectNormalized},
			Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage,
			MaxAge: 2 * day, MaxBytes: 1 << 30, Discard: jetstream.DiscardOld,
			Duplicates:  2 * time.Minute,
			Description: "Canonical observations; replay after state or rule changes.",
		},
		{
			Name: StreamInventory, Subjects: []string{SubjectInventoryObserved},
			Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage,
			MaxAge: 7 * day, MaxBytes: 64 << 20, Discard: jetstream.DiscardOld,
			Duplicates: 2 * time.Minute,
		},
		{
			Name: StreamEvents, Subjects: []string{SubjectEventDevice, SubjectEventAlert},
			Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage,
			MaxAge: 30 * day, MaxBytes: 256 << 20, Discard: jetstream.DiscardOld,
			Duplicates: 10 * time.Minute,
		},
		{
			Name: StreamAutomation, Subjects: []string{SubjectAutomationRequest, SubjectAutomationResult},
			Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage,
			MaxAge: 30 * day, MaxBytes: 256 << 20, Discard: jetstream.DiscardOld,
			Duplicates: 10 * time.Minute,
		},
		{
			Name: StreamDeadLetter, Subjects: []string{SubjectDeadLetter},
			Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage,
			MaxAge: 14 * day, MaxBytes: 128 << 20, Discard: jetstream.DiscardOld,
			Description: "Messages that could not be processed; inspect, fix, replay.",
		},
	}
}

// EnsureStreams creates or updates every stream. It is idempotent and safe to
// run from every process at startup.
func (c *Client) EnsureStreams(ctx context.Context) error {
	for _, spec := range StreamSpecs() {
		if _, err := c.js.CreateOrUpdateStream(ctx, spec); err != nil {
			return domain.Wrap(domain.CategoryDependency, "ensure stream "+spec.Name, err)
		}
	}
	return nil
}

// Publish stores payload on subject and waits for the JetStream ack. msgID
// (may be empty) is the de-duplication key: republishing the same ID inside
// the stream's duplicate window is dropped by the server. That protects
// against publisher retries only; consumers still assume at-least-once.
func (c *Client) Publish(ctx context.Context, subject, msgID string, payload []byte, headers map[string]string) error {
	msg := &nats.Msg{Subject: subject, Data: payload, Header: nats.Header{}}
	for k, v := range headers {
		msg.Header.Set(k, v)
	}
	opts := []jetstream.PublishOpt{}
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if _, err = c.js.PublishMsg(ctx, msg, opts...); err == nil {
			return nil
		}
		if ctx.Err() != nil || !retryablePublish(err) {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		}
	}
	return domain.Wrap(categoryOfPublish(err), "publish "+subject, err)
}

func retryablePublish(err error) bool {
	return errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrConnectionClosed) || errors.Is(err, nats.ErrDisconnected) ||
		errors.Is(err, jetstream.ErrNoStreamResponse)
}

func categoryOfPublish(err error) domain.Category {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
		return domain.CategoryTimeout
	case retryablePublish(err):
		return domain.CategoryDependency
	}
	return domain.CategoryPermanent
}

// Message is a delivered JetStream message plus the controls a handler needs.
type Message struct {
	msg jetstream.Msg
}

func (m Message) Data() []byte              { return m.msg.Data() }
func (m Message) Subject() string           { return m.msg.Subject() }
func (m Message) Header() nats.Header       { return m.msg.Headers() }
func (m Message) Ack() error                { return m.msg.Ack() }
func (m Message) Nak(d time.Duration) error { return m.msg.NakWithDelay(d) }
func (m Message) Term() error               { return m.msg.Term() }
func (m Message) InProgress() error         { return m.msg.InProgress() }

// Attempt is the 1-based delivery count.
func (m Message) Attempt() uint64 {
	md, err := m.msg.Metadata()
	if err != nil {
		return 1
	}
	return md.NumDelivered
}

// Handler processes one message. Returning nil acknowledges it; returning an
// error negatively acknowledges so JetStream redelivers.
type Handler func(ctx context.Context, m Message) error

// ConsumerSpec describes a durable pull consumer. Consumers sharing a Durable
// name form a consumer group: each message goes to exactly one member.
type ConsumerSpec struct {
	Stream        string
	Durable       string
	FilterSubject string
	AckWait       time.Duration
	MaxDeliver    int
	MaxAckPending int
}

// Consume attaches handler to a durable consumer and runs until ctx ends.
// Messages are acknowledged only after the handler returns nil.
func (c *Client) Consume(ctx context.Context, spec ConsumerSpec, handler Handler) error {
	cons, err := c.js.CreateOrUpdateConsumer(ctx, spec.Stream, jetstream.ConsumerConfig{
		Durable:       spec.Durable,
		FilterSubject: spec.FilterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       spec.AckWait,
		MaxDeliver:    spec.MaxDeliver,
		MaxAckPending: spec.MaxAckPending,
	})
	if err != nil {
		return domain.Wrap(domain.CategoryDependency, "create consumer "+spec.Durable, err)
	}
	cc, err := cons.Consume(func(m jetstream.Msg) {
		msg := Message{msg: m}
		if herr := handler(ctx, msg); herr != nil {
			c.log.Warn("handler failed; requesting redelivery", "subject", m.Subject(), "error", herr)
			_ = msg.Nak(time.Second)
			return
		}
		if aerr := msg.Ack(); aerr != nil {
			c.log.Warn("ack failed; message may be redelivered", "subject", m.Subject(), "error", aerr)
		}
	})
	if err != nil {
		return domain.Wrap(domain.CategoryDependency, "start consumer "+spec.Durable, err)
	}
	<-ctx.Done()
	cc.Stop()
	return nil
}

// String describes the connection for logs.
func (c *Client) String() string { return fmt.Sprintf("nats(%s)", c.nc.ConnectedUrl()) }
