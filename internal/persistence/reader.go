package persistence

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// Page selects a page of a keyset-paginated listing. Cursors are opaque
// strings produced by the store; offsets are never used because they skip or
// repeat rows while new data arrives and get slow on large tables.
type Page struct {
	Limit  int
	Cursor string
}

// EventFilter narrows an event listing. Zero fields match everything.
type EventFilter struct {
	DeviceID    string
	Type        string
	MinSeverity domain.Severity
	Since       time.Time // inclusive
	Until       time.Time // exclusive
}

// AlertFilter narrows an alert listing.
type AlertFilter struct {
	Status   domain.AlertStatus
	DeviceID string
}

// AutomationFilter narrows an automation listing.
type AutomationFilter struct {
	Status   domain.AutomationStatus
	DeviceID string
	PolicyID string
}

// AutomationView is a request together with its result, if it has one.
type AutomationView struct {
	Request domain.AutomationRequest
	Result  *domain.AutomationResult
}

// MetricPoint is one stored measurement.
type MetricPoint struct {
	ObservationID string            `json:"observation_id"`
	Metric        string            `json:"metric"`
	Value         any               `json:"value"`
	Labels        map[string]string `json:"labels,omitempty"`
	ObservedAt    time.Time         `json:"observed_at"`
}

// Reader is the read side used by the API. Both stores implement it.
type Reader interface {
	ListDevices(ctx context.Context) ([]domain.Device, error)
	// DeviceStates returns the current derived state of every tracked entity of a device.
	DeviceStates(ctx context.Context, deviceID string) ([]domain.StateRecord, error)
	ListEvents(ctx context.Context, f EventFilter, p Page) (items []domain.Event, next string, err error)
	ListAlerts(ctx context.Context, f AlertFilter, p Page) (items []domain.Alert, next string, err error)
	ListAutomation(ctx context.Context, f AutomationFilter, p Page) (items []AutomationView, next string, err error)
	// Metrics returns points of one metric of a device, newest first.
	Metrics(ctx context.Context, deviceID, metric string, since, until time.Time, limit int) ([]MetricPoint, error)
	// LatestMetrics returns the most recent point of every series of a device.
	LatestMetrics(ctx context.Context, deviceID string) ([]MetricPoint, error)
	GetAutomation(ctx context.Context, id string) (*domain.AutomationRequest, *domain.AutomationResult, error)
}

type cursor struct {
	T  time.Time `json:"t"`
	ID string    `json:"id"`
}

// EncodeCursor makes an opaque keyset cursor from a sort key and tie-breaker.
func EncodeCursor(t time.Time, id string) string {
	b, _ := json.Marshal(cursor{T: t.UTC(), ID: id})
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses a cursor; an empty string decodes to a zero cursor.
func DecodeCursor(s string) (t time.Time, id string, err error) {
	if s == "" {
		return time.Time{}, "", nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, "", domain.Errorf(domain.CategoryValidation, "invalid cursor")
	}
	var c cursor
	if err := json.Unmarshal(b, &c); err != nil || c.ID == "" {
		return time.Time{}, "", domain.Errorf(domain.CategoryValidation, "invalid cursor")
	}
	return c.T, c.ID, nil
}

// severitiesFrom lists severities at or above min.
func severitiesFrom(min domain.Severity) []string {
	var out []string
	for _, s := range []domain.Severity{domain.SeverityInfo, domain.SeverityWarning, domain.SeverityCritical} {
		if s.Rank() >= min.Rank() {
			out = append(out, string(s))
		}
	}
	return out
}

func clampLimit(n int) int {
	switch {
	case n <= 0:
		return 50
	case n > 1000:
		return 1000
	}
	return n
}
