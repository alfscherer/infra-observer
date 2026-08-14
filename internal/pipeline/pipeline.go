// Package pipeline wires the processing stages together.
//
// Two stages run as separate NATS consumers, with a durable stream between them:
//
//	telemetry.raw.*  -> [validate -> normalize]        -> telemetry.normalized
//	telemetry.normalized -> [enrich -> state -> persist] -> outbox -> events.*
//
// Stage A depends only on the message and the mapping table, so the
// normalized stream can be regenerated from raw by replay. Stage B reads
// mutable inventory and writes state; it can be replayed from normalized after
// a state definition or rule change.
//
// Each component is individually testable: this package only orders them and
// owns the transaction boundary.
package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/enrich"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/normalize"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/rules"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/state"
)

// Extensions are the optional script hooks. Implementations must isolate
// script failures themselves (a broken script leaves the observation
// unchanged) and return an error only for platform conditions, which are
// retried.
type Extensions interface {
	Transform(ctx context.Context, o domain.Observation) (domain.Observation, bool, error)
	Enrich(ctx context.Context, o domain.Observation, dev domain.Device) (domain.Observation, error)
}

// Processor holds the stage components. Zero values of the optional fields
// disable the corresponding behaviour.
type Processor struct {
	Validator  schema.Validator
	Normalizer *normalize.Normalizer
	Enricher   enrich.Enricher
	States     *state.Definitions
	Rules      *rules.Rules // optional
	Ext        Extensions   // optional: JavaScript transforms and enrichers
	Store      persistence.Store
	Log        *slog.Logger
	Now        func() time.Time

	// Stale-data detection.
	StaleAfter    time.Duration
	SweepInterval time.Duration
	StartedAt     time.Time
}

func (p *Processor) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Processor) log() *slog.Logger {
	if p.Log != nil {
		return p.Log.With("component", "pipeline")
	}
	return slog.Default().With("component", "pipeline")
}

// Normalized is the output of stage A.
type Normalized struct {
	Observation domain.Observation
	Payload     []byte
	Mapped      bool
	Dropped     bool // a transform script dropped the observation on purpose
}

// NormalizeMessage is stage A: strict decode, validation, normalization and a
// final validation of the result. Any error here is a validation error: the
// message is poison and must be dead-lettered, not retried.
func (p *Processor) NormalizeMessage(ctx context.Context, data []byte) (Normalized, error) {
	obs, err := p.Validator.DecodeObservation(data)
	if err != nil {
		return Normalized{}, err
	}
	if obs.ReceivedAt.IsZero() {
		obs.ReceivedAt = p.now().UTC()
	}
	norm, res, err := p.Normalizer.Normalize(obs)
	if err != nil {
		return Normalized{}, err
	}
	if p.Ext != nil {
		next, keep, err := p.Ext.Transform(ctx, norm)
		if err != nil {
			return Normalized{}, err
		}
		if !keep {
			return Normalized{Observation: norm, Dropped: true}, nil
		}
		norm = next
	}
	if err := p.Validator.Validate(norm); err != nil {
		return Normalized{}, err
	}
	payload, err := schema.Encode(norm)
	if err != nil {
		return Normalized{}, err
	}
	return Normalized{Observation: norm, Payload: payload, Mapped: res == normalize.Mapped}, nil
}

// Outcome describes what processing one observation did.
type Outcome struct {
	Duplicate   bool   // observation ID was already stored: everything skipped
	Dropped     string // reason the observation was intentionally not processed
	Transitions int
	Events      []domain.Event
	Ignored     int // state samples ignored as stale/duplicate/wrong type
}

// Process is stage B for one normalized observation. All of its side effects
// happen inside one storage transaction, so redelivery is safe: the first
// statement is the duplicate gate, and everything after it commits or rolls
// back together.
func (p *Processor) Process(ctx context.Context, o domain.Observation) (Outcome, error) {
	enriched, dev, err := p.Enricher.Enrich(o)
	switch {
	case errors.Is(err, enrich.ErrDeviceDisabled):
		return Outcome{Dropped: "device_disabled"}, nil
	case err != nil:
		return Outcome{}, err
	}
	if p.Ext != nil {
		if enriched, err = p.Ext.Enrich(ctx, enriched, dev); err != nil {
			return Outcome{}, err
		}
	}
	defs := p.States.For(enriched.Metric, dev.DeviceType)

	var out Outcome
	err = p.Store.Do(ctx, func(tx persistence.Tx) error {
		out = Outcome{} // Do may retry fn; start clean each attempt
		inserted, err := tx.InsertObservation(ctx, enriched)
		if err != nil {
			return err
		}
		if !inserted {
			out.Duplicate = true
			return nil
		}
		if enriched.Source != "sweeper" { // synthetic samples are not evidence the device is alive
			if err := tx.TouchDevice(ctx, enriched.DeviceID, enriched.ObservedAt); err != nil {
				return err
			}
		}
		apply := func(up state.Update) error {
			if up.Ignored != state.NotIgnored {
				out.Ignored++
				return nil
			}
			if err := tx.PutState(ctx, up.Record); err != nil {
				return err
			}
			for _, tr := range up.Transitions {
				if err := tx.InsertTransition(ctx, tr); err != nil {
					return err
				}
				out.Transitions++
			}
			for _, ev := range up.Events {
				if err := recordEvent(ctx, tx, ev); err != nil {
					return err
				}
				out.Events = append(out.Events, ev)
			}
			return nil
		}
		for _, def := range defs {
			key, _ := state.KeyFor(def, enriched.DeviceID, enriched.Labels)
			prev, err := tx.GetState(ctx, key)
			if err != nil {
				return err
			}
			if err := apply(state.Evaluate(def, prev, enriched)); err != nil {
				return err
			}
		}
		for _, rule := range p.Rules.For(enriched.Metric, dev) {
			key, _ := rules.KeyFor(rule, enriched.DeviceID, enriched.Labels)
			prev, err := tx.GetState(ctx, key)
			if err != nil {
				return err
			}
			if prev != nil && (prev.LastObservationID == enriched.ObservationID || !enriched.ObservedAt.After(prev.LastObservedAt)) {
				out.Ignored++ // duplicate or out of order: skip the window query entirely
				continue
			}
			samples, err := tx.Samples(ctx, enriched.DeviceID, enriched.Metric, domain.LabelsKey(enriched.Labels),
				enriched.ObservedAt.Add(-rule.Window()), enriched.ObservedAt)
			if err != nil {
				return err
			}
			if err := apply(rules.Evaluate(rule, prev, samples, enriched)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return Outcome{}, err
	}
	return out, nil
}

// PersistEvent stores an event produced outside the state engine (for example
// by an integration script) and queues its notification, atomically and
// idempotently on the event ID.
func PersistEvent(ctx context.Context, store persistence.Store, ev domain.Event) error {
	return store.Do(ctx, func(tx persistence.Tx) error { return recordEvent(ctx, tx, ev) })
}

// recordEvent persists an event and, only if it is new, queues its
// notifications. The event ID is deterministic, so a replayed observation
// finds the event already present and queues nothing.
func recordEvent(ctx context.Context, tx persistence.Tx, ev domain.Event) error {
	inserted, err := tx.InsertEvent(ctx, ev)
	if err != nil || !inserted {
		return err
	}
	payload, err := schema.Encode(ev)
	if err != nil {
		return err
	}
	headers := map[string]string{messaging.HeaderSchema: schema.EventV1, messaging.HeaderCorrelationID: ev.CorrelationID}
	if err := tx.Enqueue(ctx, persistence.OutboxMessage{Subject: messaging.SubjectEventDevice, MsgID: ev.EventID, Payload: payload, Headers: headers}); err != nil {
		return err
	}
	if ev.AlertKey == "" {
		return nil
	}
	var changed bool
	if ev.Resolves {
		changed, err = tx.ResolveAlert(ctx, ev)
	} else {
		changed, err = tx.OpenAlert(ctx, ev)
	}
	if err != nil || !changed {
		return err
	}
	// Both subjects live in one stream, and JetStream de-duplicates per stream
	// on Nats-Msg-Id, so the two notifications need distinct ids.
	return tx.Enqueue(ctx, persistence.OutboxMessage{Subject: messaging.SubjectEventAlert, MsgID: ev.EventID + ".alert", Payload: payload, Headers: headers})
}

// Sweep detects stale data: entities whose real samples stopped arriving are fed
// synthetic failed samples so they walk the same state machine as real
// failures. Devices that have never reported are treated as last seen at
// processor start. It returns how many synthetic samples were processed.
func (p *Processor) Sweep(ctx context.Context, now time.Time, devices []domain.Device) (int, error) {
	recs, err := p.Store.ListStates(ctx)
	if err != nil {
		return 0, err
	}
	have := map[string]bool{}
	for _, r := range recs {
		have[r.Key] = true
	}
	for _, dev := range devices {
		if !dev.Enabled || dev.Collection.Protocol != "snmp" {
			continue
		}
		for _, def := range p.States.All() {
			if !def.Stale {
				continue
			}
			key, labels := state.KeyFor(def, dev.ID, nil)
			if have[key] {
				continue
			}
			recs = append(recs, domain.StateRecord{Key: key, DeviceID: dev.ID, DefinitionID: def.ID, Labels: labels, LastSeenAt: p.StartedAt})
		}
	}
	samples := state.StaleSamples(p.States, recs, now, p.StaleAfter, p.SweepInterval)
	n := 0
	for _, s := range samples {
		if _, err := p.Process(ctx, s); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
