package domain

import (
	"time"
)

// Observation is one measured fact about a device, at one instant.
//
// Collectors emit observations and nothing else. An observation is not a
// state, not an event and not an alert; deriving those is the job of later
// stages. The same struct is used for raw (source-specific metric names) and
// normalized (canonical metric names) observations; the stage that holds it
// determines which vocabulary Metric uses.
type Observation struct {
	// ObservationID is stable: the same measurement redelivered carries the
	// same ID, which is what consumers use to detect duplicates.
	ObservationID string `json:"observation_id"`
	// CorrelationID ties every artefact derived from a collection cycle together.
	CorrelationID string `json:"correlation_id"`
	DeviceID      string `json:"device_id"`
	// Source names the mechanism, e.g. "snmp", "script:vendor-http".
	Source     string            `json:"source"`
	Metric     string            `json:"metric"`
	Value      any               `json:"value"` // float64, bool or string
	Labels     map[string]string `json:"labels,omitempty"`
	ObservedAt time.Time         `json:"observed_at"`
	ReceivedAt time.Time         `json:"received_at,omitzero"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// NewObservationID derives the stable ID for a measurement taken during a
// collection cycle. Re-polling produces a new cycle and therefore a new ID;
// redelivering the same message reproduces the same ID.
func NewObservationID(cycleID, deviceID, metric string, labels map[string]string) string {
	return StableID("obs", cycleID, deviceID, metric, LabelsKey(labels))
}

// Float returns the value as a float64 when it is numeric or boolean.
func (o Observation) Float() (float64, bool) {
	switch v := o.Value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// Bool returns the value as a bool when it is boolean.
func (o Observation) Bool() (bool, bool) {
	b, ok := o.Value.(bool)
	return b, ok
}

// Clone returns a copy that shares no maps with o, so stages can mutate freely.
func (o Observation) Clone() Observation {
	o.Labels = cloneMap(o.Labels)
	o.Metadata = cloneMap(o.Metadata)
	return o
}

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	c := make(map[string]string, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}
