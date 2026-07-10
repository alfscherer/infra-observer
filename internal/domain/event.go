package domain

import "time"

// Severity ranks how much attention an event deserves.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Rank orders severities; unknown values rank lowest.
func (s Severity) Rank() int {
	switch s {
	case SeverityInfo:
		return 1
	case SeverityWarning:
		return 2
	case SeverityCritical:
		return 3
	}
	return 0
}

// Valid reports whether s is a known severity.
func (s Severity) Valid() bool { return s.Rank() > 0 }

// Event records something meaningful that happened: a state transition or a
// rule firing. Events are only produced on transitions, never per sample.
type Event struct {
	// EventID is deterministic in (device, type, labels, triggering observation),
	// so replaying the same observation cannot create a second event.
	EventID       string            `json:"event_id"`
	CorrelationID string            `json:"correlation_id"`
	DeviceID      string            `json:"device_id"`
	Type          string            `json:"type"` // e.g. "interface.down"
	Severity      Severity          `json:"severity"`
	Message       string            `json:"message"`
	Labels        map[string]string `json:"labels,omitempty"`
	OccurredAt    time.Time         `json:"occurred_at"`
	// ObservationID is the observation that triggered the event.
	ObservationID string `json:"observation_id,omitempty"`
	// Source is the definition that produced it: a state definition or rule id.
	Source string `json:"source"`
	// AlertKey groups an opening event with its recovery. Empty for
	// informational events that never open an alert.
	AlertKey string `json:"alert_key,omitempty"`
	// Resolves marks a recovery event that closes the alert with AlertKey.
	Resolves bool `json:"resolves,omitempty"`
}

// AlertStatus is the lifecycle of an alert.
type AlertStatus string

const (
	AlertFiring   AlertStatus = "firing"
	AlertResolved AlertStatus = "resolved"
)

// Alert is a long-lived record opened by an event and closed by its recovery.
type Alert struct {
	AlertKey    string            `json:"alert_key"`
	DeviceID    string            `json:"device_id"`
	Source      string            `json:"source"`
	Severity    Severity          `json:"severity"`
	Status      AlertStatus       `json:"status"`
	Summary     string            `json:"summary"`
	Labels      map[string]string `json:"labels,omitempty"`
	OpenEventID string            `json:"open_event_id"`
	OpenedAt    time.Time         `json:"opened_at"`
	ResolvedAt  time.Time         `json:"resolved_at,omitzero"`
}
