-- Core schema: inventory mirror, observations, derived state, events, alerts,
-- and the transactional outbox. Applied by `infra-observer migrate`.

CREATE TABLE devices (
    id                 text PRIMARY KEY,
    hostname           text NOT NULL,
    management_address text NOT NULL,
    device_type        text NOT NULL,
    vendor             text NOT NULL DEFAULT '',
    model              text NOT NULL DEFAULT '',
    serial_number      text NOT NULL DEFAULT '',
    site               text NOT NULL DEFAULT '',
    tags               text[] NOT NULL DEFAULT '{}',
    capabilities       text[] NOT NULL DEFAULT '{}',
    credentials_ref    text NOT NULL DEFAULT '',
    enabled            boolean NOT NULL DEFAULT true,
    collection         jsonb NOT NULL DEFAULT '{}',
    attributes         jsonb NOT NULL DEFAULT '{}',
    last_seen          timestamptz,
    updated_at         timestamptz NOT NULL DEFAULT now()
);

-- One row per observation. The primary key is the idempotency gate: inserting
-- the same observation twice is detected here, in the same transaction as
-- every other side effect.
CREATE TABLE observations (
    observation_id text PRIMARY KEY,
    correlation_id text NOT NULL,
    device_id      text NOT NULL,
    source         text NOT NULL,
    metric         text NOT NULL,
    value_num      double precision,
    value_bool     boolean,
    value_text     text,
    labels         jsonb NOT NULL DEFAULT '{}',
    labels_key     text NOT NULL DEFAULT '',
    observed_at    timestamptz NOT NULL,
    received_at    timestamptz,
    metadata       jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX observations_series_idx ON observations (device_id, metric, labels_key, observed_at DESC);
CREATE INDEX observations_observed_at_idx ON observations (observed_at);
CREATE INDEX observations_correlation_idx ON observations (correlation_id);

CREATE TABLE state_current (
    key                 text PRIMARY KEY,
    device_id           text NOT NULL,
    definition_id       text NOT NULL,
    labels              jsonb NOT NULL DEFAULT '{}',
    state               text NOT NULL,
    failures            integer NOT NULL DEFAULT 0,
    successes           integer NOT NULL DEFAULT 0,
    breached            boolean NOT NULL DEFAULT false,
    bad_since           timestamptz,
    good_since          timestamptz,
    last_observed_at    timestamptz NOT NULL,
    last_seen_at        timestamptz NOT NULL,
    last_observation_id text NOT NULL,
    last_transition_at  timestamptz NOT NULL,
    last_value          text NOT NULL DEFAULT '',
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX state_current_device_idx ON state_current (device_id);

CREATE TABLE state_transitions (
    transition_id  text PRIMARY KEY,
    key            text NOT NULL,
    device_id      text NOT NULL,
    definition_id  text NOT NULL,
    labels         jsonb NOT NULL DEFAULT '{}',
    from_state     text NOT NULL,
    to_state       text NOT NULL,
    at             timestamptz NOT NULL,
    observation_id text NOT NULL,
    correlation_id text NOT NULL
);
CREATE INDEX state_transitions_key_idx ON state_transitions (key, at DESC);

CREATE TABLE events (
    event_id       text PRIMARY KEY,
    correlation_id text NOT NULL,
    device_id      text NOT NULL,
    type           text NOT NULL,
    severity       text NOT NULL,
    message        text NOT NULL,
    labels         jsonb NOT NULL DEFAULT '{}',
    occurred_at    timestamptz NOT NULL,
    observation_id text NOT NULL DEFAULT '',
    source         text NOT NULL,
    alert_key      text NOT NULL DEFAULT '',
    resolves       boolean NOT NULL DEFAULT false,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_device_idx ON events (device_id, occurred_at DESC);
CREATE INDEX events_occurred_idx ON events (occurred_at DESC);
CREATE INDEX events_correlation_idx ON events (correlation_id);

CREATE TABLE alerts (
    open_event_id    text PRIMARY KEY,
    alert_key        text NOT NULL,
    device_id        text NOT NULL,
    source           text NOT NULL,
    severity         text NOT NULL,
    status           text NOT NULL,
    summary          text NOT NULL,
    labels           jsonb NOT NULL DEFAULT '{}',
    opened_at        timestamptz NOT NULL,
    resolved_at      timestamptz,
    resolve_event_id text NOT NULL DEFAULT ''
);
-- At most one firing alert per alert key: a second opening event for an alert
-- that is already firing cannot create a duplicate.
CREATE UNIQUE INDEX alerts_one_firing_idx ON alerts (alert_key) WHERE status = 'firing';
CREATE INDEX alerts_device_idx ON alerts (device_id, opened_at DESC);

-- Messages waiting to be published. Written in the same transaction as the
-- state change that produced them, so a crash cannot lose an event.
CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    subject      text NOT NULL,
    msg_id       text NOT NULL,
    payload      bytea NOT NULL,
    headers      jsonb NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;
