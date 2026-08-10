package pipeline

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/enrich"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
)

// scriptedHarness wires the shipped scripts into the processor. extraDir, if
// set, holds additional scripts (used to inject broken ones).
func (h *harness) withScripts(t *testing.T, extraDirs ...string) *scripting.Service {
	t.Helper()
	cfg, err := config.Load("../../configs/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Scripting.Directories = append([]string{"../../scripts"}, extraDirs...)
	cfg.Scripting.QuarantineAfter = 1000
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	devices := []domain.Device{
		{ID: "switch-01", Hostname: "switch-01.lab", ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Site: "lab", Enabled: true,
			Collection: domain.CollectionSpec{Protocol: "snmp"}, Attributes: map[string]string{"interface.Gi0/1.description": "uplink to switch-02"}},
		{ID: "ap-01", Hostname: "ap-01.lab", ManagementAddress: "x", DeviceType: domain.DeviceAccessPoint, Site: "lab", Enabled: true,
			Collection: domain.CollectionSpec{Protocol: "snmp"}},
	}
	_ = h.store.UpsertDevices(context.Background(), devices)
	h.p.Enricher = enrich.Enricher{Inventory: inventory.NewRegistry(devices)}
	svc, rep := scripting.New(cfg.Scripting, cfg.Scripts, []runtime.Installer{api.Installer(api.Deps{Inventory: h.p.Enricher.Inventory, Log: log})}, log)
	t.Cleanup(svc.Close)
	if rep.Loaded < 3 {
		t.Fatalf("shipped scripts did not load: %+v", rep)
	}
	h.p.Ext = &scripting.Extensions{Svc: svc, Validator: h.p.Validator, Log: log}
	return svc
}

func rawObs(id, dev, metric string, value any, labels map[string]string, at time.Time) []byte {
	b, _ := schema.Encode(domain.Observation{
		ObservationID: id, CorrelationID: "corr-" + id, DeviceID: dev, Source: "snmp", Metric: metric, Value: value, Labels: labels, ObservedAt: at,
	})
	return b
}

func TestScriptTransformNormalizesVendorCPUAndTemperature(t *testing.T) {
	h := newHarness(t)
	h.withScripts(t)
	ctx := context.Background()

	cpu, err := h.p.NormalizeMessage(ctx, rawObs("r1", "ap-01", "vendor.cpu.load", 97.0, nil, t0))
	if err != nil || cpu.Dropped {
		t.Fatalf("%v %+v", err, cpu)
	}
	if cpu.Observation.Metric != "system.cpu.utilization" || cpu.Observation.Value != 0.97 || cpu.Observation.Metadata["raw_metric"] != "vendor.cpu.load" {
		t.Fatalf("%+v", cpu.Observation)
	}
	if cpu.Observation.CorrelationID != "corr-r1" || cpu.Observation.ObservationID != "r1" {
		t.Fatal("scripts must not disturb tracing identity")
	}
	temp, _ := h.p.NormalizeMessage(ctx, rawObs("r2", "ap-01", "vendor.temperature.decic", 823.0, nil, t0))
	if temp.Observation.Metric != "environment.temperature.celsius" || temp.Observation.Value != 82.3 {
		t.Fatalf("%+v", temp.Observation)
	}
	// unrelated metrics never reach JavaScript
	uptime, _ := h.p.NormalizeMessage(ctx, rawObs("r3", "ap-01", "snmp.sysUpTime", 5.0, nil, t0))
	if uptime.Observation.Metric != "system.uptime" {
		t.Fatalf("%+v", uptime.Observation)
	}
}

func TestScriptNormalizedMetricsDriveStateAndEvents(t *testing.T) {
	h := newHarness(t)
	h.withScripts(t)
	ctx := context.Background()
	var events []domain.Event
	for i := 1; i <= 3; i++ { // 2 consecutive samples above 65C raise temperature.high
		at := t0.Add(time.Duration(i) * 10 * time.Second)
		n, err := h.p.NormalizeMessage(ctx, rawObs("temp-"+string(rune('0'+i)), "ap-01", "vendor.temperature.decic", 823.0, nil, at))
		if err != nil {
			t.Fatal(err)
		}
		out, err := h.p.Process(ctx, n.Observation)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, out.Events...)
	}
	if len(events) != 1 || events[0].Type != "temperature.high" || events[0].DeviceID != "ap-01" {
		t.Fatalf("the vendor metric only becomes an event because a script normalized it: %+v", events)
	}
}

func TestScriptDropStopsTheObservation(t *testing.T) {
	h := newHarness(t)
	h.withScripts(t)
	n, err := h.p.NormalizeMessage(context.Background(), rawObs("bad", "ap-01", "vendor.cpu.load", "n/a", nil, t0))
	if err != nil || !n.Dropped || len(n.Payload) != 0 {
		t.Fatalf("a script returning null drops the observation without an error: %+v %v", n, err)
	}
}

func TestScriptEnrichmentAddsRoleAfterBuiltinEnrichment(t *testing.T) {
	h := newHarness(t)
	h.withScripts(t)
	ctx := context.Background()
	n, _ := h.p.NormalizeMessage(ctx, rawObs("if1", "switch-01", "snmp.ifOperStatus", 1.0, map[string]string{"ifIndex": "1", "ifName": "Gi0/1"}, t0))
	if _, err := h.p.Process(ctx, n.Observation); err != nil {
		t.Fatal(err)
	}
	stored, ok := h.store.Observation("if1")
	if !ok {
		t.Fatal("observation not stored")
	}
	if stored.Metadata["interface_description"] != "uplink to switch-02" || stored.Metadata["interface_role"] != "uplink" || stored.Metadata["site"] != "lab" {
		t.Fatalf("Go enrichment first, script enrichment second, both persisted: %v", stored.Metadata)
	}
}

func TestBrokenScriptsDoNotStopTheirNeighboursOrThePipeline(t *testing.T) {
	extra := t.TempDir()
	if err := os.MkdirAll(filepath.Join(extra, "transforms"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"00-throws": `throw new Error("bug in my script")`,
		"01-spins":  `for(;;){}`,
		"02-lies":   `return {...o, device_id: "somebody-else"}`,
	} {
		src := "export const meta = {version: \"1\", metrics: [\"vendor.cpu.load\"]}\nexport function transform(o) {\n" + body + "\n}\n"
		if err := os.WriteFile(filepath.Join(extra, "transforms", name+".js"), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	h := newHarness(t)
	svc := h.withScripts(t, extra)
	svc.Pool.Close()
	svc.Pool = runtime.NewPool(svc.Registry, runtime.Options{Workers: 2, QueueSize: 8, Timeout: 60 * time.Millisecond, Installers: nil})

	n, err := h.p.NormalizeMessage(context.Background(), rawObs("r1", "ap-01", "vendor.cpu.load", 50.0, nil, t0))
	if err != nil {
		t.Fatalf("script failures must never fail the pipeline: %v", err)
	}
	if n.Observation.Metric != "system.cpu.utilization" || n.Observation.Value != 0.5 || n.Observation.DeviceID != "ap-01" {
		t.Fatalf("the shipped, working transform must still apply: %+v", n.Observation)
	}
	failing := 0
	for _, s := range svc.Registry.List() {
		if strings.HasPrefix(s.ID, "0") && s.TotalFailures == 1 {
			failing++
		}
	}
	if failing != 3 {
		t.Fatalf("each broken script should have recorded exactly one failure, got %d", failing)
	}
}
