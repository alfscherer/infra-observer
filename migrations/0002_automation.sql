-- Automation requests and results: the audit trail of every attempt to act.

CREATE TABLE automation_requests (
    request_id     text PRIMARY KEY,   -- deterministic in (policy, event): a redelivered event cannot create a second request
    correlation_id text NOT NULL,
    policy_id      text NOT NULL,
    event_id       text NOT NULL,
    alert_key      text NOT NULL DEFAULT '',
    device_id      text NOT NULL,
    action         text NOT NULL,
    target         text NOT NULL DEFAULT '',
    params         jsonb NOT NULL DEFAULT '{}',
    reason         text NOT NULL DEFAULT '',
    proposer       text NOT NULL,
    dry_run        boolean NOT NULL,
    status         text NOT NULL,
    not_before     timestamptz NOT NULL,
    created_at     timestamptz NOT NULL,
    approved_by    text NOT NULL DEFAULT '',
    claimed_at     timestamptz,
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX automation_requests_due_idx ON automation_requests (status, not_before);
CREATE INDEX automation_requests_history_idx ON automation_requests (policy_id, device_id, target, created_at DESC);
CREATE INDEX automation_requests_alert_idx ON automation_requests (policy_id, alert_key);
CREATE INDEX automation_requests_device_idx ON automation_requests (device_id, created_at DESC);
CREATE INDEX automation_requests_correlation_idx ON automation_requests (correlation_id);

CREATE TABLE automation_results (
    request_id     text PRIMARY KEY REFERENCES automation_requests (request_id),
    correlation_id text NOT NULL,
    status         text NOT NULL,
    dry_run        boolean NOT NULL,
    message        text NOT NULL,
    details        jsonb NOT NULL DEFAULT '{}',
    started_at     timestamptz NOT NULL,
    finished_at    timestamptz NOT NULL
);
