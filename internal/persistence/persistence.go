// Package persistence defines the storage contract of the platform and its two
// implementations: PostgreSQL for real use and an in-memory store for fast
// unit tests. Both run the same contract suite (storetest), so the in-memory
// double cannot quietly drift from the real thing.
//
// The unit of work is Store.Do: a function that receives a Tx and either
// commits everything or nothing. The processing pipeline does all of one
// observation's side effects inside one Do, which is what makes redelivery safe.
package persistence

import (
	"context"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// OutboxMessage is a message queued for publication to NATS.
type OutboxMessage struct {
	ID      int64
	Subject string
	MsgID   string
	Payload []byte
	Headers map[string]string
}

// Tx is the set of operations available inside one atomic unit of work.
type Tx interface {
	// InsertObservation stores o. inserted is false if the observation ID already
	// exists, meaning this message is a duplicate delivery.
	InsertObservation(ctx context.Context, o domain.Observation) (inserted bool, err error)
	// TouchDevice advances the device's last_seen; it never moves it backwards.
	TouchDevice(ctx context.Context, deviceID string, at time.Time) error

	// Samples returns the stored numeric/boolean points of one series with
	// from < observed_at <= to, oldest first. The upper bound makes rule
	// evaluation depend only on data at or before the observation being
	// processed, which keeps it deterministic under replay and reordering.
	Samples(ctx context.Context, deviceID, metric, labelsKey string, from, to time.Time) ([]domain.Sample, error)

	// GetState returns the record for key, or nil. It locks the key for the
	// remainder of the transaction so concurrent workers serialise per entity.
	GetState(ctx context.Context, key string) (*domain.StateRecord, error)
	PutState(ctx context.Context, r domain.StateRecord) error
	// InsertTransition is idempotent on TransitionID.
	InsertTransition(ctx context.Context, t domain.Transition) error

	// InsertEvent is idempotent on EventID; inserted is false for a duplicate.
	InsertEvent(ctx context.Context, e domain.Event) (inserted bool, err error)
	// OpenAlert opens an alert unless one is already firing for the key.
	OpenAlert(ctx context.Context, e domain.Event) (opened bool, err error)
	// ResolveAlert closes the firing alert for e.AlertKey, if any.
	ResolveAlert(ctx context.Context, e domain.Event) (resolved bool, err error)

	// Enqueue adds a message to the outbox.
	Enqueue(ctx context.Context, m OutboxMessage) error
}

// Store is the storage entry point.
type Store interface {
	// Do runs fn atomically. If fn returns an error nothing it did is kept.
	// Transient database failures (deadlock, serialisation) are retried by the
	// implementation; the returned error is categorised.
	Do(ctx context.Context, fn func(Tx) error) error

	// UpsertDevices mirrors registered devices into storage without touching last_seen.
	UpsertDevices(ctx context.Context, devices []domain.Device) error
	// ListDevices returns mirrored devices including last_seen, ordered by id.
	ListDevices(ctx context.Context) ([]domain.Device, error)
	// Latest returns the most recent stored observation of a series, or nil.
	// A nil labels map matches any label set; a non-nil map (even empty) must
	// match the series' labels exactly.
	Latest(ctx context.Context, deviceID, metric string, labels map[string]string) (*domain.Observation, error)
	// ListStates returns every current state record (used by the stale sweeper).
	ListStates(ctx context.Context) ([]domain.StateRecord, error)

	// DrainOutbox claims up to limit unpublished messages and calls publish. If
	// publish returns nil they are marked published; otherwise they stay queued.
	// Concurrent drainers never receive the same message. It returns the number
	// of messages published.
	DrainOutbox(ctx context.Context, limit int, publish func(context.Context, []OutboxMessage) error) (int, error)

	// Ping verifies the backing database is reachable.
	Ping(ctx context.Context) error
	Close()
}
