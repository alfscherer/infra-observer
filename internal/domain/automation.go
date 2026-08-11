package domain

import "time"

// AutomationStatus is the lifecycle of an automation request.
type AutomationStatus string

const (
	AutomationPending          AutomationStatus = "pending"           // waiting for not_before / conditions
	AutomationAwaitingApproval AutomationStatus = "awaiting_approval" // policy demands a human
	AutomationApproved         AutomationStatus = "approved"
	AutomationExecuting        AutomationStatus = "executing" // claimed by a worker
	AutomationSucceeded        AutomationStatus = "succeeded"
	AutomationFailed           AutomationStatus = "failed"
	AutomationDryRun           AutomationStatus = "dry_run" // executed against the adapter in dry-run mode
	AutomationDenied           AutomationStatus = "denied"  // rejected by policy; nothing ran
	AutomationExpired          AutomationStatus = "expired" // condition no longer holds
)

// Terminal reports whether no further processing will happen.
func (s AutomationStatus) Terminal() bool {
	switch s {
	case AutomationSucceeded, AutomationFailed, AutomationDryRun, AutomationDenied, AutomationExpired:
		return true
	}
	return false
}

// AutomationRequest is the audited intent to perform one action. It is always
// created by the Go policy engine, even when a script proposed the action.
type AutomationRequest struct {
	RequestID     string            `json:"request_id"`
	CorrelationID string            `json:"correlation_id"`
	PolicyID      string            `json:"policy_id"`
	EventID       string            `json:"event_id"`
	AlertKey      string            `json:"alert_key,omitempty"`
	DeviceID      string            `json:"device_id"`
	Action        string            `json:"action"`
	Target        string            `json:"target,omitempty"`
	Params        map[string]string `json:"params,omitempty"`
	Reason        string            `json:"reason,omitempty"`
	// Proposer is "policy" for a static action or "script:<id>" for a proposal.
	Proposer   string           `json:"proposer"`
	DryRun     bool             `json:"dry_run"`
	Status     AutomationStatus `json:"status"`
	NotBefore  time.Time        `json:"not_before"`
	CreatedAt  time.Time        `json:"created_at"`
	ApprovedBy string           `json:"approved_by,omitempty"`
}

// AutomationResult is the audited outcome of a request.
type AutomationResult struct {
	RequestID     string            `json:"request_id"`
	CorrelationID string            `json:"correlation_id"`
	Status        AutomationStatus  `json:"status"`
	DryRun        bool              `json:"dry_run"`
	Message       string            `json:"message"`
	Details       map[string]string `json:"details,omitempty"`
	StartedAt     time.Time         `json:"started_at"`
	FinishedAt    time.Time         `json:"finished_at"`
}
