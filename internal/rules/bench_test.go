package rules

import (
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/domain"
)

func BenchmarkEvaluateAverage30Samples(b *testing.B) {
	r := Rule{ID: "cpu", Match: Match{Metric: "m"}, Severity: domain.SeverityWarning,
		Condition: Condition{AverageOver: 5 * time.Minute, GreaterThan: f64(0.9), MinSamples: 3}}
	s := numSamples(make([]float64, 30)...)
	o := ob(30)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Evaluate(r, nil, s, o)
	}
}
