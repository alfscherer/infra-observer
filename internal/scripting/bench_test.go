package scripting

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
)

// nativeCPU is the Go equivalent of scripts/transforms/normalize-cpu.js.
func nativeCPU(o domain.Observation) domain.Observation {
	v, ok := o.Value.(float64)
	if !ok || o.Metric != "vendor.cpu.load" {
		return o
	}
	o = o.Clone()
	o.Metric, o.Value = "system.cpu.utilization", min(max(v/100, 0), 1)
	if o.Metadata == nil {
		o.Metadata = map[string]string{}
	}
	o.Metadata["raw_metric"], o.Metadata["unit"] = "vendor.cpu.load", "ratio"
	return o
}

func benchExt(b *testing.B, workers int) (*Extensions, registry.Script, domain.Observation) {
	src, err := os.ReadFile("../../scripts/transforms/normalize-cpu.js")
	if err != nil {
		b.Fatal(err)
	}
	dir := b.TempDir()
	writeScript(&testing.T{}, dir, "transforms", "normalize-cpu", string(src))
	cfg := config.Default().Scripting
	cfg.Directories, cfg.Workers, cfg.QueueSize = []string{dir}, workers, 1024
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, rep := New(cfg, nil, nil, log)
	if rep.Failed != 0 {
		b.Fatal(rep.Errors)
	}
	b.Cleanup(svc.Close)
	sc, _ := svc.Registry.Get("transforms/normalize-cpu")
	return &Extensions{Svc: svc, Validator: schema.NewValidator(), Log: log}, sc, obs("vendor.cpu.load", 40.0)
}

func BenchmarkTransformNativeGo(b *testing.B) {
	o := obs("vendor.cpu.load", 40.0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		nativeCPU(o)
	}
}

// Full extension path: JSON in, interpreter call, JSON out, strict decode,
// validation and immutability checks.
func BenchmarkTransformJavaScript1Worker(b *testing.B) {
	ext, sc, o := benchExt(b, 1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := ext.RunTransform(context.Background(), sc, o); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTransformJavaScript4WorkersParallel(b *testing.B) {
	ext, sc, o := benchExt(b, 4)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := ext.RunTransform(context.Background(), sc, o); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
