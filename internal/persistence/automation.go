package persistence

import (
	"context"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// AutomationStore persists automation requests and results. Both Store
// implementations provide it. Methods that produce notifications take outbox
// messages and enqueue them in the same transaction as the state change, for
// the same reason processing does: no event without a record, no record whose
// notification can be lost.
type AutomationStore interface {
	// InsertAutomationRequest stores a request; inserted is false if the
	// deterministic request ID already exists (a redelivered event).
	InsertAutomationRequest(ctx context.Context, req domain.AutomationRequest, outbox ...OutboxMessage) (inserted bool, err error)

	// DueAutomation returns requests that can be worked on: pending ones whose
	// not_before has passed, approved ones, and ones whose executor died
	// mid-flight (status executing with a claim older than lease).
	DueAutomation(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]domain.AutomationRequest, error)

	// ClaimAutomation moves a request to executing, exclusively: of several
	// workers racing for it, exactly one gets true.
	ClaimAutomation(ctx context.Context, id string, now time.Time, lease time.Duration) (bool, error)

	// SetAutomationStatus changes status only if the current status is in from.
	SetAutomationStatus(ctx context.Context, id string, from []domain.AutomationStatus, to domain.AutomationStatus) (bool, error)

	// ApproveAutomation moves awaiting_approval to approved and records who did it.
	ApproveAutomation(ctx context.Context, id, by string) (bool, error)

	// CompleteAutomation records the result and sets the request's final status.
	// Completing twice is a no-op (the second call returns false).
	CompleteAutomation(ctx context.Context, res domain.AutomationResult, outbox ...OutboxMessage) (bool, error)

	// AwaitingApprovalBefore lists requests waiting for approval since before cutoff.
	AwaitingApprovalBefore(ctx context.Context, cutoff time.Time) ([]domain.AutomationRequest, error)

	// AutomationHistory returns executed requests (succeeded, failed or dry_run)
	// for the same policy, device and target whose execution finished at or after
	// since. It is how cooldowns are enforced: from execution time, not from when
	// the request was created (a request may wait a long time to become due).
	AutomationHistory(ctx context.Context, policyID, deviceID, target string, since time.Time) ([]domain.AutomationRequest, error)

	// AttemptsForAlert counts executed requests of a policy for one alert.
	AttemptsForAlert(ctx context.Context, policyID, alertKey string) (int, error)

	// AlertFiring reports whether the alert is currently firing.
	AlertFiring(ctx context.Context, alertKey string) (bool, error)

	// GetAutomation returns a request and its result (nil until completed).
	GetAutomation(ctx context.Context, id string) (*domain.AutomationRequest, *domain.AutomationResult, error)
}
