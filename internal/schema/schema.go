// Package schema defines the wire format of platform messages and the
// validation applied at trust boundaries.
//
// Messages are JSON with the schema version carried in the X-Schema header
// (see messaging.HeaderSchema). Decoding is strict: unknown fields, trailing
// data and oversize payloads are rejected as validation errors, which the
// consumer treats as poison (dead-letter, never retry).
package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// Schema identifiers, one per message type and version.
const (
	ObservationV1       = "observation.v1"
	InventoryV1         = "inventory.v1"
	EventV1             = "event.v1"
	AutomationRequestV1 = "automation.request.v1"
	AutomationResultV1  = "automation.result.v1"
	DeadLetterV1        = "deadletter.v1"
)

// Limits bound message size and shape so one bad producer cannot exhaust memory
// or bloat storage.
type Limits struct {
	MaxMessageBytes int
	MaxLabels       int
	MaxMetadata     int
	MaxKeyLen       int
	MaxValueLen     int
	MaxMetricLen    int
	MaxFutureSkew   time.Duration
}

// DefaultLimits are conservative for infrastructure telemetry.
func DefaultLimits() Limits {
	return Limits{
		MaxMessageBytes: 64 << 10, MaxLabels: 16, MaxMetadata: 32,
		MaxKeyLen: 64, MaxValueLen: 1024, MaxMetricLen: 128, MaxFutureSkew: 5 * time.Minute,
	}
}

var (
	metricPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*(\.[A-Za-z0-9_]+)*$`)
	keyPattern    = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]*$`)
)

// Validator checks observations against Limits.
type Validator struct {
	Limits Limits
	Now    func() time.Time
}

// NewValidator returns a Validator with default limits.
func NewValidator() Validator { return Validator{Limits: DefaultLimits()} }

func (v Validator) now() time.Time {
	if v.Now != nil {
		return v.Now()
	}
	return time.Now()
}

// Validate rejects structurally invalid observations. It has no maximum age:
// replayed history must remain valid; staleness is a state-engine concern.
func (v Validator) Validate(o domain.Observation) error {
	l := v.Limits
	var p []string
	if o.ObservationID == "" {
		p = append(p, "observation_id is required")
	}
	if o.CorrelationID == "" {
		p = append(p, "correlation_id is required")
	}
	if o.DeviceID == "" {
		p = append(p, "device_id is required")
	}
	if o.Source == "" {
		p = append(p, "source is required")
	}
	if o.Metric == "" || len(o.Metric) > l.MaxMetricLen || !metricPattern.MatchString(o.Metric) {
		p = append(p, fmt.Sprintf("metric %q is not a valid metric name", o.Metric))
	}
	switch val := o.Value.(type) {
	case nil:
		p = append(p, "value is required")
	case bool:
	case string:
		if len(val) > l.MaxValueLen {
			p = append(p, fmt.Sprintf("string value exceeds %d bytes", l.MaxValueLen))
		}
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			p = append(p, "numeric value must be finite")
		}
	case int, int32, int64, uint32, uint64, float32:
	default:
		p = append(p, fmt.Sprintf("value has unsupported type %T (want number, boolean or string)", o.Value))
	}
	if o.ObservedAt.IsZero() {
		p = append(p, "observed_at is required")
	} else if o.ObservedAt.After(v.now().Add(l.MaxFutureSkew)) {
		p = append(p, fmt.Sprintf("observed_at %s is more than %s in the future", o.ObservedAt.Format(time.RFC3339), l.MaxFutureSkew))
	}
	p = append(p, checkMap("labels", o.Labels, l.MaxLabels, l)...)
	p = append(p, checkMap("metadata", o.Metadata, l.MaxMetadata, l)...)
	if len(p) > 0 {
		return domain.Errorf(domain.CategoryValidation, "invalid observation %q: %s", o.ObservationID, strings.Join(p, "; "))
	}
	return nil
}

func checkMap(name string, m map[string]string, maxN int, l Limits) []string {
	var p []string
	if len(m) > maxN {
		p = append(p, fmt.Sprintf("%s has %d entries, max %d", name, len(m), maxN))
	}
	for k, val := range m {
		if len(k) > l.MaxKeyLen || !keyPattern.MatchString(k) {
			p = append(p, fmt.Sprintf("%s key %q is invalid", name, k))
		}
		if len(val) > l.MaxValueLen {
			p = append(p, fmt.Sprintf("%s value for %q exceeds %d bytes", name, k, l.MaxValueLen))
		}
	}
	return p
}

// Encode serialises a message. Observation values are normalised so that
// integer types encode identically to floats.
func Encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, domain.Wrap(domain.CategoryValidation, "encode", err)
	}
	return b, nil
}

// Decode strictly parses data into T.
func Decode[T any](data []byte, maxBytes int) (T, error) {
	var out T
	if maxBytes > 0 && len(data) > maxBytes {
		return out, domain.Errorf(domain.CategoryValidation, "message is %d bytes, limit %d", len(data), maxBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return out, domain.Errorf(domain.CategoryValidation, "malformed message: %v", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return out, domain.Errorf(domain.CategoryValidation, "malformed message: trailing data")
	}
	return out, nil
}

// DecodeObservation parses and validates an observation in one step. This is
// the first stage of the pipeline: nothing downstream sees an unvalidated one.
func (v Validator) DecodeObservation(data []byte) (domain.Observation, error) {
	o, err := Decode[domain.Observation](data, v.Limits.MaxMessageBytes)
	if err != nil {
		return o, err
	}
	return o, v.Validate(o)
}
