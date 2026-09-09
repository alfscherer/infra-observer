package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkNormalizeMessageStageA(b *testing.B) {
	h := newHarness(&testing.T{})
	payload := rawObs("r1", "switch-01", "snmp.ifOperStatus", 1.0, map[string]string{"ifIndex": "1", "ifName": "Gi0/1"}, t0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := h.p.NormalizeMessage(context.Background(), payload); err != nil {
			b.Fatal(err)
		}
	}
}

// Stage B against the in-memory store: pipeline logic without database cost.
func BenchmarkProcessStageBMemStore(b *testing.B) {
	h := newHarness(&testing.T{})
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		o := ifObs(i+1, i%2 == 0)
		o.ObservationID = fmt.Sprint("b", i)
		o.ObservedAt = t0.Add(time.Duration(i) * time.Second)
		if _, err := h.p.Process(ctx, o); err != nil {
			b.Fatal(err)
		}
	}
}
