package domain

import "time"

// HealthState is the position of one tracked entity in the health machine.
//
//	UP -> SUSPECT -> DOWN -> RECOVERING -> UP
type HealthState string

const (
	StateUp         HealthState = "UP"
	StateSuspect    HealthState = "SUSPECT"
	StateDown       HealthState = "DOWN"
	StateRecovering HealthState = "RECOVERING"
)

// StateRecord is the current derived state of one entity, for example one
// interface of one switch, or one device's reachability.
type StateRecord struct {
	Key          string            `json:"key"` // definition/device/labels
	DeviceID     string            `json:"device_id"`
	DefinitionID string            `json:"definition_id"`
	Labels       map[string]string `json:"labels,omitempty"`
	State        HealthState       `json:"state"`
	Failures     int               `json:"failures"`  // consecutive bad samples
	Successes    int               `json:"successes"` // consecutive good samples while DOWN/RECOVERING
	// Breached remembers which side of a hysteresis band a numeric metric is on.
	Breached          bool      `json:"breached"`
	BadSince          time.Time `json:"bad_since,omitzero"`  // first sample of the current bad streak
	GoodSince         time.Time `json:"good_since,omitzero"` // first sample of the current good streak
	LastObservedAt    time.Time `json:"last_observed_at"`    // last sample applied, real or synthetic
	LastSeenAt        time.Time `json:"last_seen_at"`        // last *real* sample; drives stale-data detection
	LastObservationID string    `json:"last_observation_id"`
	LastTransitionAt  time.Time `json:"last_transition_at"`
	LastValue         string    `json:"last_value,omitempty"`
}

// Transition records one change of HealthState. Transitions are the audit trail
// of the state engine; not every transition becomes an event.
type Transition struct {
	TransitionID  string            `json:"transition_id"`
	Key           string            `json:"key"`
	DeviceID      string            `json:"device_id"`
	DefinitionID  string            `json:"definition_id"`
	Labels        map[string]string `json:"labels,omitempty"`
	From          HealthState       `json:"from"` // empty on first adoption
	To            HealthState       `json:"to"`
	At            time.Time         `json:"at"`
	ObservationID string            `json:"observation_id"`
	CorrelationID string            `json:"correlation_id"`
}
