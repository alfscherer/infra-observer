package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// PGStore is the PostgreSQL implementation of Store.
type PGStore struct {
	pool *pgxpool.Pool
	// OnError, when set, is called for every database error (metrics hook).
	OnError func(err error)
}

// NewPGStore wraps an existing pool.
func NewPGStore(pool *pgxpool.Pool) *PGStore { return &PGStore{pool: pool} }

// Connect opens a pool and verifies connectivity.
func Connect(ctx context.Context, url string, maxConns int) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, domain.Wrap(domain.CategoryValidation, "parse database url", err)
	}
	cfg.MaxConns = int32(maxConns)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, domain.Wrap(domain.CategoryDependency, "open database pool", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, domain.Wrap(domain.CategoryDependency, "ping database", err)
	}
	return pool, nil
}

func (s *PGStore) Close() { s.pool.Close() }

func (s *PGStore) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return s.fail("ping", err)
	}
	return nil
}

func (s *PGStore) fail(op string, err error) error {
	out := classifyPG(op, err)
	if s.OnError != nil {
		s.OnError(out)
	}
	return out
}

// classifyPG maps database errors onto platform categories. Connection loss,
// deadlocks and serialisation failures are retryable; constraint and data
// errors are not: retrying an invalid row forever helps nobody.
func classifyPG(op string, err error) error {
	var de *domain.Error
	if errors.As(err, &de) {
		return err
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) && len(pe.Code) >= 2 {
		switch pe.Code[:2] {
		case "22", "23", "42":
			return domain.Wrap(domain.CategoryPermanent, op, err)
		default:
			return domain.Wrap(domain.CategoryDependency, op, err)
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.Wrap(domain.CategoryTimeout, op, err)
	}
	var ne net.Error
	if errors.As(err, &ne) || errors.Is(err, context.Canceled) {
		return domain.Wrap(domain.CategoryDependency, op, err)
	}
	return domain.Wrap(domain.CategoryDependency, op, err)
}

func retryablePG(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && (pe.Code == "40001" || pe.Code == "40P01")
}

func (s *PGStore) Do(ctx context.Context, fn func(Tx) error) error {
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		err = s.doOnce(ctx, fn)
		if err == nil || !retryablePG(err) {
			break
		}
		select {
		case <-ctx.Done():
			return s.fail("transaction", ctx.Err())
		case <-time.After(time.Duration(attempt+1) * 20 * time.Millisecond):
		}
	}
	if err == nil {
		return nil
	}
	return s.fail("transaction", err)
}

func (s *PGStore) doOnce(ctx context.Context, fn func(Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(&pgTx{tx: tx}); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return err
	}
	return tx.Commit(ctx)
}

type pgTx struct{ tx pgx.Tx }

func js(v any) string {
	if v == nil {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	if string(b) == "null" {
		return "{}"
	}
	return string(b)
}

func nilTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (t *pgTx) InsertObservation(ctx context.Context, o domain.Observation) (bool, error) {
	var num *float64
	var bl *bool
	var txt *string
	switch v := o.Value.(type) {
	case bool:
		bl = &v
		f := 0.0
		if v {
			f = 1
		}
		num = &f
	case string:
		txt = &v
	default:
		if f, ok := o.Float(); ok {
			num = &f
		}
	}
	tag, err := t.tx.Exec(ctx, `
		INSERT INTO observations (observation_id, correlation_id, device_id, source, metric,
			value_num, value_bool, value_text, labels, labels_key, observed_at, received_at, metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13::jsonb)
		ON CONFLICT (observation_id) DO NOTHING`,
		o.ObservationID, o.CorrelationID, o.DeviceID, o.Source, o.Metric, num, bl, txt,
		js(o.Labels), domain.LabelsKey(o.Labels), o.ObservedAt, nilTime(o.ReceivedAt), js(o.Metadata))
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (t *pgTx) TouchDevice(ctx context.Context, id string, at time.Time) error {
	_, err := t.tx.Exec(ctx, `UPDATE devices SET last_seen = GREATEST(COALESCE(last_seen, $2), $2) WHERE id = $1`, id, at)
	return err
}

func (t *pgTx) Samples(ctx context.Context, deviceID, metric, labelsKey string, from, to time.Time) ([]domain.Sample, error) {
	rows, err := t.tx.Query(ctx, `
		SELECT observed_at, value_num, value_bool FROM observations
		WHERE device_id = $1 AND metric = $2 AND labels_key = $3 AND observed_at > $4 AND observed_at <= $5
		  AND value_num IS NOT NULL
		ORDER BY observed_at`, deviceID, metric, labelsKey, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Sample
	for rows.Next() {
		var s domain.Sample
		if err := rows.Scan(&s.At, &s.Num, &s.Bool); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (t *pgTx) GetState(ctx context.Context, key string) (*domain.StateRecord, error) {
	// The advisory lock serialises workers on this key even when the row does
	// not exist yet, which FOR UPDATE alone cannot do.
	if _, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return nil, err
	}
	var (
		r                   domain.StateRecord
		labels              []byte
		badSince, goodSince *time.Time
	)
	err := t.tx.QueryRow(ctx, `
		SELECT key, device_id, definition_id, labels, state, failures, successes, breached, bad_since, good_since,
		       last_observed_at, last_seen_at, last_observation_id, last_transition_at, last_value
		FROM state_current WHERE key = $1`, key).Scan(
		&r.Key, &r.DeviceID, &r.DefinitionID, &labels, &r.State, &r.Failures, &r.Successes, &r.Breached,
		&badSince, &goodSince, &r.LastObservedAt, &r.LastSeenAt, &r.LastObservationID, &r.LastTransitionAt, &r.LastValue)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.Labels = decodeLabels(labels)
	if badSince != nil {
		r.BadSince = *badSince
	}
	if goodSince != nil {
		r.GoodSince = *goodSince
	}
	return &r, nil
}

func decodeLabels(b []byte) map[string]string {
	if len(b) == 0 {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(b, &m); err != nil || len(m) == 0 {
		return nil
	}
	return m
}

func (t *pgTx) PutState(ctx context.Context, r domain.StateRecord) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO state_current (key, device_id, definition_id, labels, state, failures, successes, breached,
			bad_since, good_since, last_observed_at, last_seen_at, last_observation_id, last_transition_at, last_value, updated_at)
		VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15, now())
		ON CONFLICT (key) DO UPDATE SET state=EXCLUDED.state, failures=EXCLUDED.failures, successes=EXCLUDED.successes,
			breached=EXCLUDED.breached, bad_since=EXCLUDED.bad_since, good_since=EXCLUDED.good_since,
			last_observed_at=EXCLUDED.last_observed_at, last_seen_at=EXCLUDED.last_seen_at,
			last_observation_id=EXCLUDED.last_observation_id, last_transition_at=EXCLUDED.last_transition_at,
			last_value=EXCLUDED.last_value, updated_at=now()`,
		r.Key, r.DeviceID, r.DefinitionID, js(r.Labels), string(r.State), r.Failures, r.Successes, r.Breached,
		nilTime(r.BadSince), nilTime(r.GoodSince), r.LastObservedAt, r.LastSeenAt, r.LastObservationID, r.LastTransitionAt, r.LastValue)
	return err
}

func (t *pgTx) InsertTransition(ctx context.Context, tr domain.Transition) error {
	_, err := t.tx.Exec(ctx, `
		INSERT INTO state_transitions (transition_id, key, device_id, definition_id, labels, from_state, to_state, at, observation_id, correlation_id)
		VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10) ON CONFLICT (transition_id) DO NOTHING`,
		tr.TransitionID, tr.Key, tr.DeviceID, tr.DefinitionID, js(tr.Labels), string(tr.From), string(tr.To), tr.At, tr.ObservationID, tr.CorrelationID)
	return err
}

func (t *pgTx) InsertEvent(ctx context.Context, e domain.Event) (bool, error) {
	tag, err := t.tx.Exec(ctx, `
		INSERT INTO events (event_id, correlation_id, device_id, type, severity, message, labels, occurred_at, observation_id, source, alert_key, resolves)
		VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12) ON CONFLICT (event_id) DO NOTHING`,
		e.EventID, e.CorrelationID, e.DeviceID, e.Type, string(e.Severity), e.Message, js(e.Labels), e.OccurredAt, e.ObservationID, e.Source, e.AlertKey, e.Resolves)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (t *pgTx) OpenAlert(ctx context.Context, e domain.Event) (bool, error) {
	tag, err := t.tx.Exec(ctx, `
		INSERT INTO alerts (open_event_id, alert_key, device_id, source, severity, status, summary, labels, opened_at)
		VALUES ($1,$2,$3,$4,$5,'firing',$6,$7::jsonb,$8) ON CONFLICT DO NOTHING`,
		e.EventID, e.AlertKey, e.DeviceID, e.Source, string(e.Severity), e.Message, js(e.Labels), e.OccurredAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (t *pgTx) ResolveAlert(ctx context.Context, e domain.Event) (bool, error) {
	tag, err := t.tx.Exec(ctx, `
		UPDATE alerts SET status='resolved', resolved_at=$2, resolve_event_id=$3
		WHERE alert_key=$1 AND status='firing'`, e.AlertKey, e.OccurredAt, e.EventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (t *pgTx) Enqueue(ctx context.Context, m OutboxMessage) error {
	_, err := t.tx.Exec(ctx, `INSERT INTO outbox (subject, msg_id, payload, headers) VALUES ($1,$2,$3,$4::jsonb)`,
		m.Subject, m.MsgID, m.Payload, js(m.Headers))
	return err
}

func (s *PGStore) UpsertDevices(ctx context.Context, devices []domain.Device) error {
	batch := &pgx.Batch{}
	for _, d := range devices {
		batch.Queue(`
			INSERT INTO devices (id, hostname, management_address, device_type, vendor, model, serial_number, site,
				tags, capabilities, credentials_ref, enabled, collection, attributes, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb, now())
			ON CONFLICT (id) DO UPDATE SET hostname=EXCLUDED.hostname, management_address=EXCLUDED.management_address,
				device_type=EXCLUDED.device_type, vendor=EXCLUDED.vendor, model=EXCLUDED.model,
				serial_number=EXCLUDED.serial_number, site=EXCLUDED.site, tags=EXCLUDED.tags,
				capabilities=EXCLUDED.capabilities, credentials_ref=EXCLUDED.credentials_ref, enabled=EXCLUDED.enabled,
				collection=EXCLUDED.collection, attributes=EXCLUDED.attributes, updated_at=now()`,
			d.ID, d.Hostname, d.ManagementAddress, string(d.DeviceType), d.Vendor, d.Model, d.SerialNumber, d.Site,
			nonNil(d.Tags), nonNil(d.Capabilities), d.CredentialsRef, d.Enabled, js(d.Collection), js(d.Attributes))
	}
	res := s.pool.SendBatch(ctx, batch)
	defer res.Close()
	for range devices {
		if _, err := res.Exec(); err != nil {
			return s.fail("upsert devices", err)
		}
	}
	return nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func (s *PGStore) ListDevices(ctx context.Context) ([]domain.Device, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, hostname, management_address, device_type, vendor, model, serial_number, site, tags, capabilities,
		       credentials_ref, enabled, collection, attributes, last_seen
		FROM devices ORDER BY id`)
	if err != nil {
		return nil, s.fail("list devices", err)
	}
	defer rows.Close()
	var out []domain.Device
	for rows.Next() {
		var (
			d          domain.Device
			typ        string
			coll, attr []byte
			seen       *time.Time
		)
		if err := rows.Scan(&d.ID, &d.Hostname, &d.ManagementAddress, &typ, &d.Vendor, &d.Model, &d.SerialNumber, &d.Site,
			&d.Tags, &d.Capabilities, &d.CredentialsRef, &d.Enabled, &coll, &attr, &seen); err != nil {
			return nil, s.fail("scan device", err)
		}
		d.DeviceType = domain.DeviceType(typ)
		_ = json.Unmarshal(coll, &d.Collection)
		d.Attributes = decodeLabels(attr)
		if seen != nil {
			d.LastSeen = *seen
		}
		if len(d.Tags) == 0 {
			d.Tags = nil
		}
		if len(d.Capabilities) == 0 {
			d.Capabilities = nil
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, s.fail("list devices", err)
	}
	return out, nil
}

func (s *PGStore) ListStates(ctx context.Context) ([]domain.StateRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT key, device_id, definition_id, labels, state, failures, successes, breached, bad_since, good_since,
		       last_observed_at, last_seen_at, last_observation_id, last_transition_at, last_value
		FROM state_current ORDER BY key`)
	if err != nil {
		return nil, s.fail("list states", err)
	}
	defer rows.Close()
	var out []domain.StateRecord
	for rows.Next() {
		var (
			r                   domain.StateRecord
			labels              []byte
			badSince, goodSince *time.Time
		)
		if err := rows.Scan(&r.Key, &r.DeviceID, &r.DefinitionID, &labels, &r.State, &r.Failures, &r.Successes, &r.Breached,
			&badSince, &goodSince, &r.LastObservedAt, &r.LastSeenAt, &r.LastObservationID, &r.LastTransitionAt, &r.LastValue); err != nil {
			return nil, s.fail("scan state", err)
		}
		r.Labels = decodeLabels(labels)
		if badSince != nil {
			r.BadSince = *badSince
		}
		if goodSince != nil {
			r.GoodSince = *goodSince
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, s.fail("list states", err)
	}
	return out, nil
}

// DrainOutbox claims rows with FOR UPDATE SKIP LOCKED, so several relays can run
// side by side without publishing the same row twice; rows are marked
// published only after publish succeeds, in the same transaction.
func (s *PGStore) DrainOutbox(ctx context.Context, limit int, publish func(context.Context, []OutboxMessage) error) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, s.fail("outbox begin", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck
	rows, err := tx.Query(ctx, `
		SELECT id, subject, msg_id, payload, headers FROM outbox
		WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, s.fail("outbox claim", err)
	}
	var batch []OutboxMessage
	for rows.Next() {
		var m OutboxMessage
		var headers []byte
		if err := rows.Scan(&m.ID, &m.Subject, &m.MsgID, &m.Payload, &headers); err != nil {
			rows.Close()
			return 0, s.fail("outbox scan", err)
		}
		_ = json.Unmarshal(headers, &m.Headers)
		batch = append(batch, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, s.fail("outbox claim", err)
	}
	if len(batch) == 0 {
		return 0, nil
	}
	if err := publish(ctx, batch); err != nil {
		return 0, err
	}
	ids := make([]int64, len(batch))
	for i, m := range batch {
		ids[i] = m.ID
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, s.fail("outbox mark", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, s.fail("outbox commit", err)
	}
	return len(batch), nil
}

// String is useful in logs.
func (s *PGStore) String() string {
	return fmt.Sprintf("postgres(max_conns=%d)", s.pool.Config().MaxConns)
}
