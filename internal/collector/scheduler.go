// Package collector schedules polls and hands the resulting observations to a
// Sink. It contains no business rules: it does not decide that a device is
// down, only that a poll failed, and it reports that fact as an observation.
package collector

import (
	"context"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/alfscherer/infra-observer/internal/collector/snmp"
	"github.com/alfscherer/infra-observer/internal/domain"
)

// Metric names the collector itself emits about its own polls.
const (
	MetricPollSuccess  = "collector.poll_success"
	MetricPollDuration = "collector.poll_duration_seconds"
)

// InventoryFact carries slowly-changing identity details for inventory.observed.
type InventoryFact struct {
	DeviceID      string    `json:"device_id"`
	CorrelationID string    `json:"correlation_id"`
	SysName       string    `json:"sys_name,omitempty"`
	SysDescr      string    `json:"sys_descr,omitempty"`
	SysObjectID   string    `json:"sys_object_id,omitempty"`
	ProfileID     string    `json:"profile_id,omitempty"`
	ObservedAt    time.Time `json:"observed_at"`
}

// Sink receives collector output. The production implementation publishes to
// NATS; tests capture in memory.
type Sink interface {
	Observations(ctx context.Context, obs []domain.Observation) error
	Inventory(ctx context.Context, fact InventoryFact) error
}

// PollerFunc adapts the SNMP poller (or a fake) to the scheduler.
type Poller interface {
	Poll(ctx context.Context, dev domain.Device, cycleID string) (snmp.Result, error)
}

// PollEvent is passed to the optional OnPoll hook for metrics.
type PollEvent struct {
	Device   domain.Device
	Duration time.Duration
	Err      error
	Skipped  int
}

// Scheduler polls devices on their configured intervals with a hard cap on
// concurrency. It never starts a second poll for a device that is still being
// polled, so a slow device cannot pile up work.
type Scheduler struct {
	Devices         func() []domain.Device
	Poller          Poller
	Sink            Sink
	Workers         int
	DefaultInterval time.Duration
	Log             *slog.Logger
	OnPoll          func(PollEvent)
	Now             func() time.Time

	mu       sync.Mutex
	inflight map[string]bool
}

func (s *Scheduler) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Scheduler) log() *slog.Logger {
	if s.Log != nil {
		return s.Log.With("component", "collector")
	}
	return slog.Default().With("component", "collector")
}

func (s *Scheduler) interval(d domain.Device) time.Duration {
	if d.Collection.Interval > 0 {
		return d.Collection.Interval
	}
	return s.DefaultInterval
}

// offset spreads first polls across the interval deterministically so a
// restart does not make every device fire in the same instant.
func offset(id string, interval time.Duration) time.Duration {
	h := fnv.New32a()
	h.Write([]byte(id))
	return time.Duration(uint64(h.Sum32()) % uint64(interval))
}

// Run polls until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	s.mu.Lock()
	s.inflight = map[string]bool{}
	s.mu.Unlock()

	jobs := make(chan domain.Device)
	var wg sync.WaitGroup
	for i := 0; i < max(s.Workers, 1); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dev := range jobs {
				s.PollOnce(ctx, dev)
				s.mu.Lock()
				delete(s.inflight, dev.ID)
				s.mu.Unlock()
			}
		}()
	}
	defer func() { close(jobs); wg.Wait() }()

	next := map[string]time.Time{}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}
		now := s.now()
		wake := now.Add(time.Second)
		for _, dev := range s.Devices() {
			if !dev.Enabled || dev.Collection.Protocol != "snmp" {
				continue
			}
			iv := s.interval(dev)
			due, known := next[dev.ID]
			if !known {
				due = now.Add(offset(dev.ID, iv))
				next[dev.ID] = due
			}
			if !due.After(now) {
				s.mu.Lock()
				busy := s.inflight[dev.ID]
				if !busy {
					s.inflight[dev.ID] = true
				}
				s.mu.Unlock()
				next[dev.ID] = now.Add(iv)
				if busy {
					s.log().Warn("poll still in flight; skipping cycle", "device_id", dev.ID)
				} else {
					select {
					case jobs <- dev: // blocks while every worker is busy: that is the backpressure
					case <-ctx.Done():
						return nil
					}
				}
				due = next[dev.ID]
			}
			if due.Before(wake) {
				wake = due
			}
		}
		timer.Reset(max(time.Until(wake), 5*time.Millisecond))
	}
}

// PollOnce performs one complete collection cycle for dev and publishes the
// result. It never returns an error: every outcome, including failure, is
// published as observations so downstream stages see it.
func (s *Scheduler) PollOnce(ctx context.Context, dev domain.Device) {
	cycleID := domain.NewCorrelationID()
	started := s.now()
	pctx, cancel := context.WithTimeout(ctx, s.interval(dev))
	res, err := s.Poller.Poll(pctx, dev, cycleID)
	cancel()
	took := s.now().Sub(started)
	if s.OnPoll != nil {
		s.OnPoll(PollEvent{Device: dev, Duration: took, Err: err, Skipped: res.Skipped})
	}

	obs := res.Observations
	base := domain.Observation{
		CorrelationID: cycleID, DeviceID: dev.ID, Source: "snmp",
		Labels: map[string]string{"protocol": "snmp"}, ObservedAt: started.UTC(),
	}
	ok := base
	ok.Metric, ok.Value = MetricPollSuccess, err == nil
	ok.ObservationID = domain.NewObservationID(cycleID, dev.ID, ok.Metric, ok.Labels)
	if err != nil {
		cat := domain.CategoryOf(err)
		msg := err.Error()
		if len(msg) > 200 {
			msg = msg[:200]
		}
		ok.Metadata = map[string]string{"error_category": string(cat), "error": msg}
		obs = nil // a failed poll's partial data is not trustworthy
		s.log().Warn("poll failed", "device_id", dev.ID, "category", cat, "error", err, "duration", took)
	} else {
		dur := base
		dur.Metric, dur.Value = MetricPollDuration, took.Seconds()
		dur.ObservationID = domain.NewObservationID(cycleID, dev.ID, dur.Metric, dur.Labels)
		obs = append(obs, dur)
	}
	obs = append(obs, ok)

	if perr := s.Sink.Observations(ctx, obs); perr != nil {
		s.log().Error("publish observations failed; cycle dropped", "device_id", dev.ID, "error", perr)
		return
	}
	if err == nil && (res.Facts.SysName != "" || res.Facts.SysObjectID != "") {
		fact := InventoryFact{
			DeviceID: dev.ID, CorrelationID: cycleID, SysName: res.Facts.SysName, SysDescr: res.Facts.SysDescr,
			SysObjectID: res.Facts.SysObjectID, ProfileID: res.Facts.ProfileID, ObservedAt: started.UTC(),
		}
		if perr := s.Sink.Inventory(ctx, fact); perr != nil {
			s.log().Warn("publish inventory failed", "device_id", dev.ID, "error", perr)
		}
	}
}
