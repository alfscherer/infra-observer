package state

import (
	"strconv"
	"strings"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// IgnoreReason explains why a sample did not change anything.
type IgnoreReason string

const (
	NotIgnored IgnoreReason = ""
	Duplicate  IgnoreReason = "duplicate"   // this exact observation was already applied
	Stale      IgnoreReason = "stale"       // older than (or equal to) the last applied sample
	WrongType  IgnoreReason = "wrong_type"  // value type does not fit the definition
	NotTracked IgnoreReason = "not_tracked" // no definition applies
)

// Update is the pure result of applying one observation to one definition.
type Update struct {
	Record      domain.StateRecord
	Transitions []domain.Transition
	Events      []domain.Event
	Ignored     IgnoreReason
}

// KeyFor derives the identity of the tracked entity: the definition, the
// device and the definition's key labels.
func KeyFor(def Definition, deviceID string, labels map[string]string) (string, map[string]string) {
	var kl map[string]string
	if len(def.KeyLabels) > 0 {
		kl = make(map[string]string, len(def.KeyLabels))
		for _, k := range def.KeyLabels {
			if v, ok := labels[k]; ok {
				kl[k] = v
			}
		}
	}
	return def.ID + "/" + deviceID + "/" + domain.LabelsKey(kl), kl
}

// Evaluate applies one observation to a definition and the entity's previous
// record (nil if the entity has not been seen). It performs no I/O and reads
// no clock: identical input always yields identical output, which is what
// makes replay and duplicate delivery safe.
func Evaluate(def Definition, prev *domain.StateRecord, o domain.Observation) Update {
	key, keyLabels := KeyFor(def, o.DeviceID, o.Labels)

	if prev != nil {
		if prev.LastObservationID == o.ObservationID {
			return Update{Record: *prev, Ignored: Duplicate}
		}
		if !o.ObservedAt.After(prev.LastObservedAt) {
			return Update{Record: *prev, Ignored: Stale}
		}
	}

	rec := domain.StateRecord{
		Key: key, DeviceID: o.DeviceID, DefinitionID: def.ID, Labels: keyLabels,
	}
	if prev != nil {
		rec = *prev
	}

	good, ok := judge(def, &rec, o)
	if !ok {
		if prev != nil {
			return Update{Record: *prev, Ignored: WrongType}
		}
		return Update{Ignored: WrongType}
	}

	from := rec.State
	rec.LastObservedAt, rec.LastObservationID = o.ObservedAt, o.ObservationID
	rec.LastValue = fmtValue(o.Value)
	if o.Metadata["stale"] != "true" {
		rec.LastSeenAt = o.ObservedAt // synthetic samples must not reset the staleness clock
	}
	to := step(def, &rec, good, o.ObservedAt)

	up := Update{Record: rec}
	if to != from {
		rec.State, rec.LastTransitionAt = to, o.ObservedAt
		up.Record = rec
		up.Transitions = append(up.Transitions, domain.Transition{
			TransitionID: domain.StableID("tr", key, o.ObservationID),
			Key:          key, DeviceID: o.DeviceID, DefinitionID: def.ID, Labels: keyLabels,
			From: from, To: to, At: o.ObservedAt, ObservationID: o.ObservationID, CorrelationID: o.CorrelationID,
		})
		if ev := eventFor(def, key, from, to, o); ev != nil {
			up.Events = append(up.Events, *ev)
		}
	}
	return up
}

// judge classifies a sample as good or bad. For thresholds it also updates
// rec.Breached, which carries the hysteresis memory between samples.
func judge(def Definition, rec *domain.StateRecord, o domain.Observation) (good, ok bool) {
	if def.GoodWhen != nil {
		b, isBool := o.Value.(bool)
		if !isBool {
			return false, false
		}
		return b == *def.GoodWhen.Equals, true
	}
	f, isNum := numeric(o.Value)
	if !isNum {
		return false, false
	}
	t := def.Threshold
	if t.BreachAbove != nil {
		if rec.Breached {
			rec.Breached = !(f < *t.ClearBelow)
		} else {
			rec.Breached = f > *t.BreachAbove
		}
	} else {
		if rec.Breached {
			rec.Breached = !(f > *t.ClearAbove)
		} else {
			rec.Breached = f < *t.BreachBelow
		}
	}
	return !rec.Breached, true
}

// step advances the health machine by one sample and returns the new state.
// It maintains the streak counters and debounce timestamps on rec.
func step(def Definition, rec *domain.StateRecord, good bool, at time.Time) domain.HealthState {
	cur := rec.State
	if cur == "" { // first sight of this entity: adopt without alerting
		if good {
			rec.Failures, rec.Successes = 0, 0
			return domain.StateUp
		}
		cur = domain.StateUp // a bad first sample is judged like any bad sample from UP
	}
	if good {
		rec.Failures, rec.BadSince = 0, time.Time{}
		switch cur {
		case domain.StateUp:
			return domain.StateUp
		case domain.StateSuspect: // false alarm: nothing was ever reported
			return domain.StateUp
		case domain.StateDown, domain.StateRecovering:
			if rec.Successes == 0 {
				rec.GoodSince = at
			}
			rec.Successes++
			if rec.Successes >= def.RecoverAfter && at.Sub(rec.GoodSince) >= def.RecoverFor {
				rec.Successes, rec.GoodSince = 0, time.Time{}
				return domain.StateUp
			}
			return domain.StateRecovering
		}
		return cur
	}
	// bad sample
	rec.Successes, rec.GoodSince = 0, time.Time{}
	if rec.Failures == 0 {
		rec.BadSince = at
	}
	rec.Failures++
	switch cur {
	case domain.StateDown, domain.StateRecovering:
		// Still (or again) down. The alert opened earlier is still open, so
		// bouncing between RECOVERING and DOWN is invisible to alerting.
		return domain.StateDown
	default: // UP, SUSPECT
		if rec.Failures >= def.FailAfter && at.Sub(rec.BadSince) >= def.FailFor {
			return domain.StateDown
		}
		return domain.StateSuspect
	}
}

func eventFor(def Definition, key string, from, to domain.HealthState, o domain.Observation) *domain.Event {
	var spec EventSpec
	var resolves bool
	switch {
	case to == domain.StateDown && from != domain.StateRecovering:
		spec = def.Alert
	case to == domain.StateUp && (from == domain.StateRecovering || from == domain.StateDown):
		spec, resolves = def.Recovery, true
	default:
		return nil
	}
	labels := map[string]string{}
	for k, v := range o.Labels {
		labels[k] = v
	}
	return &domain.Event{
		EventID:       domain.StableID("evt", o.DeviceID, def.ID, domain.LabelsKey(labels), spec.Event, o.ObservationID),
		CorrelationID: o.CorrelationID,
		DeviceID:      o.DeviceID,
		Type:          spec.Event,
		Severity:      spec.Severity,
		Message:       render(spec.Message, o),
		Labels:        labels,
		OccurredAt:    o.ObservedAt,
		ObservationID: o.ObservationID,
		Source:        def.ID,
		AlertKey:      key,
		Resolves:      resolves,
	}
}

// render substitutes {device}, {value} and {<label>} placeholders.
func render(tmpl string, o domain.Observation) string {
	if tmpl == "" {
		return ""
	}
	pairs := []string{"{device}", o.DeviceID, "{value}", fmtValue(o.Value)}
	for k, v := range o.Labels {
		pairs = append(pairs, "{"+k+"}", v)
	}
	if h := o.Metadata["hostname"]; h != "" {
		pairs = append(pairs, "{hostname}", h)
	}
	return strings.NewReplacer(pairs...).Replace(tmpl)
}

func fmtValue(v any) string {
	switch x := v.(type) {
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case string:
		return x
	}
	return ""
}

// StaleSamples synthesises failed samples for stale-tracked entities that have
// not been heard from for staleAfter. IDs are deterministic per sweep bucket,
// so two processors sweeping concurrently, or one sweeping twice, produce the
// same observation and the second is dropped as a duplicate. Entities already
// DOWN are skipped: they cannot get worse.
func StaleSamples(defs *Definitions, recs []domain.StateRecord, now time.Time, staleAfter, bucket time.Duration) []domain.Observation {
	var out []domain.Observation
	stamp := now.Truncate(bucket)
	for _, rec := range recs {
		def, ok := defs.Get(rec.DefinitionID)
		if !ok || !def.Stale || rec.State == domain.StateDown {
			continue
		}
		if now.Sub(rec.LastSeenAt) < staleAfter {
			continue
		}
		var val any = false
		if def.GoodWhen != nil {
			val = !*def.GoodWhen.Equals
		}
		id := domain.StableID("stale", rec.Key, strconv.FormatInt(stamp.Unix(), 10))
		out = append(out, domain.Observation{
			ObservationID: id, CorrelationID: id, DeviceID: rec.DeviceID, Source: "sweeper",
			Metric: def.Metric, Value: val, Labels: rec.Labels, ObservedAt: stamp,
			Metadata: map[string]string{"stale": "true", "last_seen_at": rec.LastSeenAt.UTC().Format(time.RFC3339)},
		})
	}
	return out
}

// numeric extracts a number; booleans are deliberately not numbers here.
func numeric(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	}
	return 0, false
}
