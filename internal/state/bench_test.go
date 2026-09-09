package state

import (
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

func BenchmarkEvaluateReachability(b *testing.B) {
	def := reachability()
	rec := &domain.StateRecord{State: domain.StateUp, LastObservedAt: t0}
	o := domain.Observation{ObservationID: "x", DeviceID: "sw1", Metric: def.Metric, Value: true, ObservedAt: t0.Add(time.Minute)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		o.ObservedAt = o.ObservedAt.Add(time.Second)
		Evaluate(def, rec, o)
	}
}
