package api

import (
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/persistence"
)

// The API has its own wire types. Internal messages (observations, events on
// NATS, database rows) are free to change; clients see only these shapes, and
// nothing sensitive (credential references, internal ids that are not part of
// the contract) leaks through by accident.

type DeviceDTO struct {
	ID           string            `json:"id"`
	Hostname     string            `json:"hostname"`
	Address      string            `json:"management_address"`
	Type         string            `json:"device_type"`
	Vendor       string            `json:"vendor,omitempty"`
	Model        string            `json:"model,omitempty"`
	SerialNumber string            `json:"serial_number,omitempty"`
	Site         string            `json:"site,omitempty"`
	Tags         []string          `json:"tags"`
	Capabilities []string          `json:"capabilities"`
	Enabled      bool              `json:"enabled"`
	LastSeen     *time.Time        `json:"last_seen,omitempty"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	OpenAlerts   *int              `json:"open_alerts,omitempty"`
}

func deviceDTO(d domain.Device) DeviceDTO {
	out := DeviceDTO{
		ID: d.ID, Hostname: d.Hostname, Address: d.ManagementAddress, Type: string(d.DeviceType), Vendor: d.Vendor, Model: d.Model,
		SerialNumber: d.SerialNumber, Site: d.Site, Tags: nonNil(d.Tags), Capabilities: nonNil(d.Capabilities), Enabled: d.Enabled, Attributes: d.Attributes,
	}
	if !d.LastSeen.IsZero() {
		t := d.LastSeen.UTC()
		out.LastSeen = &t
	}
	return out
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

type StateDTO struct {
	Definition string            `json:"definition"`
	Labels     map[string]string `json:"labels,omitempty"`
	State      string            `json:"state"`
	Since      time.Time         `json:"since"`
	LastSeen   time.Time         `json:"last_seen"`
	LastValue  string            `json:"last_value,omitempty"`
	Failures   int               `json:"consecutive_failures"`
}

func stateDTO(r domain.StateRecord) StateDTO {
	return StateDTO{Definition: r.DefinitionID, Labels: r.Labels, State: string(r.State), Since: r.LastTransitionAt.UTC(),
		LastSeen: r.LastSeenAt.UTC(), LastValue: r.LastValue, Failures: r.Failures}
}

// DeviceStateDTO summarises a device's derived state.
type DeviceStateDTO struct {
	DeviceID string     `json:"device_id"`
	Health   string     `json:"health"` // healthy, degraded, down
	States   []StateDTO `json:"states"`
}

type EventDTO struct {
	ID            string            `json:"id"`
	CorrelationID string            `json:"correlation_id"`
	DeviceID      string            `json:"device_id"`
	Type          string            `json:"type"`
	Severity      string            `json:"severity"`
	Message       string            `json:"message"`
	Labels        map[string]string `json:"labels,omitempty"`
	OccurredAt    time.Time         `json:"occurred_at"`
	Source        string            `json:"source"`
	AlertKey      string            `json:"alert_key,omitempty"`
	Resolves      bool              `json:"resolves,omitempty"`
}

func eventDTO(e domain.Event) EventDTO {
	return EventDTO{ID: e.EventID, CorrelationID: e.CorrelationID, DeviceID: e.DeviceID, Type: e.Type, Severity: string(e.Severity), Message: e.Message,
		Labels: e.Labels, OccurredAt: e.OccurredAt.UTC(), Source: e.Source, AlertKey: e.AlertKey, Resolves: e.Resolves}
}

type AlertDTO struct {
	Key        string            `json:"key"`
	DeviceID   string            `json:"device_id"`
	Source     string            `json:"source"`
	Severity   string            `json:"severity"`
	Status     string            `json:"status"`
	Summary    string            `json:"summary"`
	Labels     map[string]string `json:"labels,omitempty"`
	OpenEvent  string            `json:"opened_by_event"`
	OpenedAt   time.Time         `json:"opened_at"`
	ResolvedAt *time.Time        `json:"resolved_at,omitempty"`
}

func alertDTO(a domain.Alert) AlertDTO {
	out := AlertDTO{Key: a.AlertKey, DeviceID: a.DeviceID, Source: a.Source, Severity: string(a.Severity), Status: string(a.Status), Summary: a.Summary,
		Labels: a.Labels, OpenEvent: a.OpenEventID, OpenedAt: a.OpenedAt.UTC()}
	if !a.ResolvedAt.IsZero() {
		t := a.ResolvedAt.UTC()
		out.ResolvedAt = &t
	}
	return out
}

type AutomationResultDTO struct {
	Status     string            `json:"status"`
	DryRun     bool              `json:"dry_run"`
	Message    string            `json:"message"`
	Details    map[string]string `json:"details,omitempty"`
	StartedAt  time.Time         `json:"started_at"`
	FinishedAt time.Time         `json:"finished_at"`
}

type AutomationDTO struct {
	ID            string               `json:"id"`
	CorrelationID string               `json:"correlation_id"`
	Policy        string               `json:"policy"`
	EventID       string               `json:"event_id"`
	DeviceID      string               `json:"device_id"`
	Action        string               `json:"action"`
	Target        string               `json:"target,omitempty"`
	Params        map[string]string    `json:"params,omitempty"`
	Reason        string               `json:"reason,omitempty"`
	Proposer      string               `json:"proposer"`
	DryRun        bool                 `json:"dry_run"`
	Status        string               `json:"status"`
	NotBefore     time.Time            `json:"not_before"`
	CreatedAt     time.Time            `json:"created_at"`
	ApprovedBy    string               `json:"approved_by,omitempty"`
	Result        *AutomationResultDTO `json:"result,omitempty"`
}

func automationDTO(v persistence.AutomationView) AutomationDTO {
	return automationFrom(v.Request, v.Result)
}

func automationFrom(r domain.AutomationRequest, res *domain.AutomationResult) AutomationDTO {
	out := AutomationDTO{ID: r.RequestID, CorrelationID: r.CorrelationID, Policy: r.PolicyID, EventID: r.EventID, DeviceID: r.DeviceID, Action: r.Action,
		Target: r.Target, Params: r.Params, Reason: r.Reason, Proposer: r.Proposer, DryRun: r.DryRun, Status: string(r.Status),
		NotBefore: r.NotBefore.UTC(), CreatedAt: r.CreatedAt.UTC(), ApprovedBy: r.ApprovedBy}
	if res != nil {
		out.Result = &AutomationResultDTO{Status: string(res.Status), DryRun: res.DryRun, Message: res.Message, Details: res.Details,
			StartedAt: res.StartedAt.UTC(), FinishedAt: res.FinishedAt.UTC()}
	}
	return out
}

type MetricDTO struct {
	Metric     string            `json:"metric"`
	Value      any               `json:"value"`
	Labels     map[string]string `json:"labels,omitempty"`
	ObservedAt time.Time         `json:"observed_at"`
}

func metricDTO(p persistence.MetricPoint) MetricDTO {
	return MetricDTO{Metric: p.Metric, Value: p.Value, Labels: p.Labels, ObservedAt: p.ObservedAt.UTC()}
}

// Page is the list envelope.
type Page[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"next_cursor"`
}

func page[T any](items []T, next string) Page[T] {
	if items == nil {
		items = []T{}
	}
	p := Page[T]{Items: items}
	if next != "" {
		p.NextCursor = &next
	}
	return p
}
