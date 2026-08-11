package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alfscherer/infra-observer/internal/domain"
)

const autoCols = `request_id, correlation_id, policy_id, event_id, alert_key, device_id, action, target, params, reason,
	proposer, dry_run, status, not_before, created_at, approved_by`

// prefixCols qualifies the request column list with a table alias.
func prefixCols(alias string) string {
	cols := strings.Split(autoCols, ",")
	for i, c := range cols {
		cols[i] = alias + strings.TrimSpace(c)
	}
	return strings.Join(cols, ", ")
}

func scanAutomation(row pgx.Row) (domain.AutomationRequest, error) {
	var (
		r      domain.AutomationRequest
		params []byte
		status string
	)
	err := row.Scan(&r.RequestID, &r.CorrelationID, &r.PolicyID, &r.EventID, &r.AlertKey, &r.DeviceID, &r.Action, &r.Target,
		&params, &r.Reason, &r.Proposer, &r.DryRun, &status, &r.NotBefore, &r.CreatedAt, &r.ApprovedBy)
	r.Status = domain.AutomationStatus(status)
	r.Params = decodeLabels(params)
	return r, err
}

func (s *PGStore) enqueueTx(ctx context.Context, tx pgx.Tx, msgs []OutboxMessage) error {
	for _, m := range msgs {
		if _, err := tx.Exec(ctx, `INSERT INTO outbox (subject, msg_id, payload, headers) VALUES ($1,$2,$3,$4::jsonb)`,
			m.Subject, m.MsgID, m.Payload, js(m.Headers)); err != nil {
			return err
		}
	}
	return nil
}

func (s *PGStore) InsertAutomationRequest(ctx context.Context, req domain.AutomationRequest, outbox ...OutboxMessage) (bool, error) {
	var inserted bool
	err := s.Do(ctx, func(t Tx) error {
		tx := t.(*pgTx).tx
		tag, err := tx.Exec(ctx, `
			INSERT INTO automation_requests (`+autoCols+`)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,$16)
			ON CONFLICT (request_id) DO NOTHING`,
			req.RequestID, req.CorrelationID, req.PolicyID, req.EventID, req.AlertKey, req.DeviceID, req.Action, req.Target,
			js(req.Params), req.Reason, req.Proposer, req.DryRun, string(req.Status), req.NotBefore, req.CreatedAt, req.ApprovedBy)
		if err != nil {
			return err
		}
		inserted = tag.RowsAffected() == 1
		if inserted {
			return s.enqueueTx(ctx, tx, outbox)
		}
		return nil
	})
	return inserted, err
}

func (s *PGStore) DueAutomation(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]domain.AutomationRequest, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+autoCols+` FROM automation_requests
		WHERE (status = 'pending' AND not_before <= $1)
		   OR status = 'approved'
		   OR (status = 'executing' AND claimed_at < $2)
		ORDER BY created_at, request_id LIMIT $3`, now, now.Add(-lease), limit)
	if err != nil {
		return nil, s.fail("due automation", err)
	}
	defer rows.Close()
	var out []domain.AutomationRequest
	for rows.Next() {
		r, err := scanAutomation(rows)
		if err != nil {
			return nil, s.fail("scan automation", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PGStore) ClaimAutomation(ctx context.Context, id string, now time.Time, lease time.Duration) (bool, error) {
	// A single conditional UPDATE is the claim: of N racing workers exactly one
	// sees a row change.
	tag, err := s.pool.Exec(ctx, `
		UPDATE automation_requests SET status = 'executing', claimed_at = $2, updated_at = now()
		WHERE request_id = $1 AND (status IN ('pending','approved') OR (status = 'executing' AND claimed_at < $3))`,
		id, now, now.Add(-lease))
	if err != nil {
		return false, s.fail("claim automation", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) SetAutomationStatus(ctx context.Context, id string, from []domain.AutomationStatus, to domain.AutomationStatus) (bool, error) {
	f := make([]string, len(from))
	for i, x := range from {
		f[i] = string(x)
	}
	tag, err := s.pool.Exec(ctx, `UPDATE automation_requests SET status = $3, updated_at = now() WHERE request_id = $1 AND status = ANY($2)`, id, f, string(to))
	if err != nil {
		return false, s.fail("set automation status", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) ApproveAutomation(ctx context.Context, id, by string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE automation_requests SET status = 'approved', approved_by = $2, updated_at = now()
		WHERE request_id = $1 AND status = 'awaiting_approval'`, id, by)
	if err != nil {
		return false, s.fail("approve automation", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) CompleteAutomation(ctx context.Context, res domain.AutomationResult, outbox ...OutboxMessage) (bool, error) {
	var done bool
	err := s.Do(ctx, func(t Tx) error {
		tx := t.(*pgTx).tx
		tag, err := tx.Exec(ctx, `
			INSERT INTO automation_results (request_id, correlation_id, status, dry_run, message, details, started_at, finished_at)
			VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8) ON CONFLICT (request_id) DO NOTHING`,
			res.RequestID, res.CorrelationID, string(res.Status), res.DryRun, res.Message, js(res.Details), res.StartedAt, res.FinishedAt)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // already completed by another delivery
		}
		if _, err := tx.Exec(ctx, `UPDATE automation_requests SET status = $2, updated_at = now() WHERE request_id = $1`, res.RequestID, string(res.Status)); err != nil {
			return err
		}
		done = true
		return s.enqueueTx(ctx, tx, outbox)
	})
	return done, err
}

func (s *PGStore) AwaitingApprovalBefore(ctx context.Context, cutoff time.Time) ([]domain.AutomationRequest, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+autoCols+` FROM automation_requests WHERE status = 'awaiting_approval' AND created_at < $1 ORDER BY created_at`, cutoff)
	if err != nil {
		return nil, s.fail("awaiting approval", err)
	}
	defer rows.Close()
	var out []domain.AutomationRequest
	for rows.Next() {
		r, err := scanAutomation(rows)
		if err != nil {
			return nil, s.fail("scan automation", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PGStore) AutomationHistory(ctx context.Context, policyID, deviceID, target string, since time.Time) ([]domain.AutomationRequest, error) {
	// Cooldowns run from when the action executed (the result's finished_at),
	// not from when the request was created: a request may wait a long time
	// for its duration condition before it runs.
	rows, err := s.pool.Query(ctx, `
		SELECT `+prefixCols("r.")+` FROM automation_requests r JOIN automation_results x USING (request_id)
		WHERE r.policy_id = $1 AND r.device_id = $2 AND r.target = $3 AND x.finished_at >= $4
		  AND r.status IN ('succeeded','failed','dry_run') ORDER BY x.finished_at`, policyID, deviceID, target, since)
	if err != nil {
		return nil, s.fail("automation history", err)
	}
	defer rows.Close()
	var out []domain.AutomationRequest
	for rows.Next() {
		r, err := scanAutomation(rows)
		if err != nil {
			return nil, s.fail("scan automation", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PGStore) AttemptsForAlert(ctx context.Context, policyID, alertKey string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM automation_requests
		WHERE policy_id = $1 AND alert_key = $2 AND status IN ('succeeded','failed','dry_run')`, policyID, alertKey).Scan(&n)
	if err != nil {
		return 0, s.fail("attempts", err)
	}
	return n, nil
}

func (s *PGStore) AlertFiring(ctx context.Context, alertKey string) (bool, error) {
	var ok bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM alerts WHERE alert_key = $1 AND status = 'firing')`, alertKey).Scan(&ok); err != nil {
		return false, s.fail("alert firing", err)
	}
	return ok, nil
}

func (s *PGStore) GetAutomation(ctx context.Context, id string) (*domain.AutomationRequest, *domain.AutomationResult, error) {
	r, err := scanAutomation(s.pool.QueryRow(ctx, `SELECT `+autoCols+` FROM automation_requests WHERE request_id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, s.fail("get automation", err)
	}
	var (
		res     domain.AutomationResult
		status  string
		details []byte
	)
	err = s.pool.QueryRow(ctx, `
		SELECT request_id, correlation_id, status, dry_run, message, details, started_at, finished_at
		FROM automation_results WHERE request_id = $1`, id).Scan(
		&res.RequestID, &res.CorrelationID, &status, &res.DryRun, &res.Message, &details, &res.StartedAt, &res.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return &r, nil, nil
	}
	if err != nil {
		return nil, nil, s.fail("get automation result", err)
	}
	res.Status = domain.AutomationStatus(status)
	res.Details = decodeLabels(details)
	_ = json.Valid(details)
	return &r, &res, nil
}
