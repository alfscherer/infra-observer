package automation

import (
	"context"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// Capability is something a device adapter can do for a device.
type Capability string

// Action is what an adapter is asked to perform. DryRun is a hard instruction:
// a dry-run execution validates and describes what it would do and must not
// change anything.
type Action struct {
	Type          string
	Target        string
	Params        map[string]string
	DryRun        bool
	RequestID     string
	CorrelationID string
}

// ActionResult is what an adapter reports back.
type ActionResult struct {
	Message string
	Details map[string]string
	Changed bool // false for dry runs and read-only actions
}

// DeviceAdapter performs catalog actions against one class of device. It is
// the only place device-specific knowledge lives; the engine, policies and
// scripts never see protocol details.
type DeviceAdapter interface {
	Capabilities(ctx context.Context, device domain.Device) ([]Capability, error)
	Execute(ctx context.Context, device domain.Device, action Action) (ActionResult, error)
}

// AdapterSet chooses the adapter for a device and action.
type AdapterSet interface {
	For(device domain.Device, action string) (DeviceAdapter, error)
}

// Proposer lets an extension propose the action for a policy. Returning nil
// means "no action". The proposal is validated by the engine exactly like a
// static one; a proposer cannot make the engine do anything the catalog and
// the policy do not already allow.
type Proposer interface {
	Propose(ctx context.Context, policy Policy, event domain.Event, device domain.Device) (*Proposal, error)
}
