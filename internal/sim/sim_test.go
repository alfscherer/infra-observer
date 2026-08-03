package sim

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/collector/snmp"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/normalize"
	"github.com/alfscherer/infra-observer/internal/secrets"
)

var quiet = slog.New(slog.DiscardHandler)

func labDevices(t *testing.T) []domain.Device {
	t.Helper()
	ds, err := inventory.LoadFile("../../configs/inventory.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
	return c.t
}

// startAgents launches one UDP agent per device on loopback and returns the
// devices with their management addresses rewritten to the real ports.
func startAgents(t *testing.T, w *World, devices []domain.Device) []domain.Device {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	out := make([]domain.Device, len(devices))
	for i, d := range devices {
		a, err := w.Listen(ctx, d.ID, "127.0.0.1:0", quiet)
		if err != nil {
			t.Fatal(err)
		}
		d.ManagementAddress = a.Addr().String()
		out[i] = d
	}
	return out
}

func realPoller(t *testing.T) *snmp.Poller {
	t.Helper()
	ps, err := snmp.LoadProfiles("../../configs/profiles")
	if err != nil {
		t.Fatal(err)
	}
	return &snmp.Poller{
		Dialer:   snmp.GoSNMPDialer{Timeout: 150 * time.Millisecond, Retries: 0, MaxRepetitions: 10},
		Profiles: ps,
		Secrets:  secrets.Static{"snmp/lab": {"version": "2c", "community": "lab-ro"}},
	}
}

func dial(t *testing.T, addr, community string) snmp.Session {
	t.Helper()
	s, err := snmp.GoSNMPDialer{Timeout: 200 * time.Millisecond, Retries: 0, MaxRepetitions: 10}.Dial(context.Background(), addr, snmp.Credential{Version: "2c", Community: community})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestAgentServesGetGetNextAndBulkWalkOverRealSNMP(t *testing.T) {
	devs := labDevices(t)
	w := NewWorld(devs, Options{Seed: 1})
	addr := startAgents(t, w, devs)[0].ManagementAddress // switch-01
	s := dial(t, addr, "lab-ro")
	ctx := context.Background()

	got, err := s.Get(ctx, []string{"1.3.6.1.2.1.1.5.0", "1.3.6.1.2.1.1.3.0", "1.3.6.1.2.1.1.99.0"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Str != "switch-01" || got[1].Kind != snmp.KindNumber || got[1].Num <= 0 || got[2].Kind != snmp.KindMissing {
		t.Fatalf("get: %+v", got)
	}
	rows, err := s.Walk(ctx, "1.3.6.1.2.1.2.2.1.8")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 8 {
		t.Fatalf("expected 8 ifOperStatus rows, got %d", len(rows))
	}
	for _, r := range rows {
		if r.Num != 1 {
			t.Fatalf("all ports start up: %+v", r)
		}
	}
	names, _ := s.Walk(ctx, "1.3.6.1.2.1.31.1.1.1.1")
	if len(names) != 8 || names[0].Str != "Gi0/1" || names[7].Str != "Gi0/8" {
		t.Fatalf("ifName column: %+v", names)
	}
	// a full walk must return OIDs in strictly ascending order (agent correctness)
	all, err := s.Walk(ctx, "1.3.6.1")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < 40 {
		t.Fatalf("expected a populated MIB view, got %d objects", len(all))
	}
	for i := 1; i < len(all); i++ {
		if cmpOID(parseOID(all[i-1].OID), parseOID(all[i].OID)) >= 0 {
			t.Fatalf("walk order violated at %s -> %s", all[i-1].OID, all[i].OID)
		}
	}
}

func TestAgentFailureModesAreObservableAsRealErrors(t *testing.T) {
	devs := labDevices(t)
	clock := &fakeClock{t: time.Now()}
	w := NewWorld(devs, Options{Seed: 1, Now: clock.Now})
	addr := startAgents(t, w, devs)[0].ManagementAddress
	ctx := context.Background()

	if _, err := dial(t, addr, "wrong").Get(ctx, []string{"1.3.6.1.2.1.1.5.0"}); domain.CategoryOf(err) != domain.CategoryTimeout {
		t.Fatalf("wrong v2c community looks like silence: %v", err)
	}
	good := dial(t, addr, "lab-ro")

	if _, err := w.Apply(Scenario{Device: "switch-01", Event: "offline"}); err != nil {
		t.Fatal(err)
	}
	if _, err := good.Get(ctx, []string{"1.3.6.1.2.1.1.5.0"}); domain.CategoryOf(err) != domain.CategoryTimeout {
		t.Fatalf("offline device: %v", err)
	}
	_, _ = w.Apply(Scenario{Device: "switch-01", Event: "online"})
	if _, err := good.Get(ctx, []string{"1.3.6.1.2.1.1.5.0"}); err != nil {
		t.Fatalf("back online: %v", err)
	}

	_, _ = w.Apply(Scenario{Device: "switch-01", Event: "auth-failure"})
	if _, err := good.Get(ctx, []string{"1.3.6.1.2.1.1.5.0"}); domain.CategoryOf(err) != domain.CategoryAuthentication {
		t.Fatalf("auth failure must be categorised as authentication, got %v", err)
	}
	_, _ = w.Apply(Scenario{Device: "switch-01", Event: "auth-ok"})

	lossy := NewWorld(devs, Options{Seed: 1, DropRate: 1})
	addr2 := startAgents(t, lossy, devs)[0].ManagementAddress
	if _, err := dial(t, addr2, "lab-ro").Get(ctx, []string{"1.3.6.1.2.1.1.5.0"}); domain.CategoryOf(err) != domain.CategoryTimeout {
		t.Fatalf("packet loss: %v", err)
	}
}

func TestEveryDeviceTypePollsCleanlyWithShippedProfiles(t *testing.T) {
	devs := labDevices(t)
	w := NewWorld(devs, Options{Seed: 3})
	devs = startAgents(t, w, devs)
	p := realPoller(t)
	norm, err := normalize.LoadFile("../../configs/normalization.yaml")
	if err != nil {
		t.Fatal(err)
	}
	scriptHandled := map[string]bool{"vendor.cpu.load": true, "vendor.temperature.decic": true}

	for _, d := range devs {
		res, err := p.Poll(context.Background(), d, "cycle")
		if err != nil {
			t.Fatalf("%s: %v", d.ID, err)
		}
		if res.Facts.SysName != d.ID || res.Facts.ProfileID != d.Collection.Profile {
			t.Errorf("%s: facts %+v", d.ID, res.Facts)
		}
		metrics := map[string]int{}
		for _, o := range res.Observations {
			metrics[o.Metric]++
			if _, kind, err := norm.Normalize(o); err != nil {
				t.Errorf("%s %s: %v", d.ID, o.Metric, err)
			} else if kind == normalize.Passthrough && !scriptHandled[o.Metric] {
				t.Errorf("%s emits %s, which has no normalization and no script transform", d.ID, o.Metric)
			}
		}
		t.Logf("%-15s %2d observations, %d metric names, %d skipped", d.ID, len(res.Observations), len(metrics), res.Skipped)
		switch d.DeviceType {
		case domain.DeviceSwitch:
			if metrics["snmp.ifOperStatus"] != 8 || metrics["snmp.cpuLoadPercent"] != 1 || metrics["snmp.temperatureCelsius"] != 1 {
				t.Errorf("%s metrics: %v", d.ID, metrics)
			}
		case domain.DeviceAccessPoint:
			if metrics["vendor.cpu.load"] != 1 || metrics["vendor.temperature.decic"] != 1 || metrics["snmp.ifOperStatus"] != 3 {
				t.Errorf("%s metrics: %v", d.ID, metrics)
			}
		case domain.DeviceServer, domain.DeviceWorkstation:
			if metrics["snmp.hrProcessorLoad"] != 2 || metrics["snmp.memoryUsedPercent"] != 1 {
				t.Errorf("%s metrics: %v", d.ID, metrics)
			}
		case domain.DeviceUPS:
			if metrics["snmp.upsOutputSource"] != 1 || metrics["snmp.upsEstimatedChargeRemaining"] != 1 {
				t.Errorf("%s metrics: %v", d.ID, metrics)
			}
		}
		if res.Skipped != 0 {
			t.Errorf("%s: %d profile metrics unavailable from the simulator", d.ID, res.Skipped)
		}
	}
}

func TestScenariosChangeWhatTheCollectorSees(t *testing.T) {
	devs := labDevices(t)
	clock := &fakeClock{t: time.Now()}
	w := NewWorld(devs, Options{Seed: 3, Now: clock.Now})
	devs = startAgents(t, w, devs)
	p := realPoller(t)
	byID := map[string]domain.Device{}
	for _, d := range devs {
		byID[d.ID] = d
	}
	value := func(dev, metric, label, want string) any {
		res, err := p.Poll(context.Background(), byID[dev], "c")
		if err != nil {
			t.Fatalf("%s: %v", dev, err)
		}
		for _, o := range res.Observations {
			if o.Metric == metric && (label == "" || o.Labels["ifName"] == label) {
				return o.Value
			}
		}
		t.Fatalf("%s: metric %s %s not found", dev, metric, label)
		return nil
	}
	if v := value("switch-01", "snmp.ifOperStatus", "Gi0/1", ""); v != 1.0 {
		t.Fatalf("baseline oper status: %v", v)
	}
	if _, err := w.Apply(Scenario{Device: "switch-01", Event: "interface-down", Args: map[string]string{"interface": "Gi0/1"}}); err != nil {
		t.Fatal(err)
	}
	if v := value("switch-01", "snmp.ifOperStatus", "Gi0/1", ""); v != 2.0 {
		t.Fatalf("interface-down not visible over SNMP: %v", v)
	}
	if v := value("switch-01", "snmp.ifOperStatus", "Gi0/2", ""); v != 1.0 {
		t.Fatal("other interfaces unaffected")
	}
	_, _ = w.Apply(Scenario{Device: "switch-01", Event: "interface-up", Args: map[string]string{"interface": "Gi0/1"}})
	if v := value("switch-01", "snmp.ifOperStatus", "Gi0/1", ""); v != 1.0 {
		t.Fatalf("interface-up: %v", v)
	}

	_, _ = w.Apply(Scenario{Device: "server-01", Event: "high-cpu"})
	w.Tick(clock.Advance(time.Second))
	if v := value("server-01", "snmp.hrProcessorLoad", "", "").(float64); v < 90 {
		t.Fatalf("high-cpu should push load above 90, got %v", v)
	}
	_, _ = w.Apply(Scenario{Device: "server-01", Event: "cpu-normal"})
	w.Tick(clock.Advance(time.Second))
	if v := value("server-01", "snmp.hrProcessorLoad", "", "").(float64); v > 60 {
		t.Fatalf("cpu-normal: %v", v)
	}

	_, _ = w.Apply(Scenario{Device: "ups-01", Event: "power-loss"})
	if v := value("ups-01", "snmp.upsOutputSource", "", ""); v != 5.0 {
		t.Fatalf("on battery expects source 5, got %v", v)
	}
	w.Tick(clock.Advance(60 * time.Second))
	if v := value("ups-01", "snmp.upsEstimatedChargeRemaining", "", "").(float64); v >= 100 {
		t.Fatalf("battery should drain on battery, got %v", v)
	}

	_, _ = w.Apply(Scenario{Device: "ap-01", Event: "overheat"})
	w.Tick(clock.Advance(time.Second))
	if v := value("ap-01", "vendor.temperature.decic", "", "").(float64); v < 700 {
		t.Fatalf("AP reports tenths of a degree, expected >700, got %v", v)
	}
}

func TestFlapTogglesOnTickAndSettlesUp(t *testing.T) {
	devs := labDevices(t)
	clock := &fakeClock{t: time.Now()}
	w := NewWorld(devs, Options{Seed: 1, Now: clock.Now})
	if _, err := w.Apply(Scenario{Device: "switch-01", Event: "interface-flap", Args: map[string]string{"interface": "Gi0/3", "toggles": "4", "interval": "5s"}}); err != nil {
		t.Fatal(err)
	}
	dev, _ := w.Device("switch-01")
	state := func() bool { var up bool; w.With(func() { up = dev.Iface[2].OperUp }); return up }
	var seen []bool
	for i := 0; i < 5; i++ {
		w.Tick(clock.Advance(5 * time.Second))
		seen = append(seen, state())
	}
	want := []bool{false, true, false, true, true} // 4 toggles, the last leaves the port up
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("flap sequence %v, want %v", seen, want)
		}
	}
}

func TestDurationAutoReverts(t *testing.T) {
	devs := labDevices(t)
	clock := &fakeClock{t: time.Now()}
	w := NewWorld(devs, Options{Seed: 1, Now: clock.Now})
	msg, err := w.Apply(Scenario{Device: "ap-01", Event: "offline", Duration: time.Minute})
	if err != nil || !strings.Contains(msg, "reverts") {
		t.Fatalf("%q %v", msg, err)
	}
	dev, _ := w.Device("ap-01")
	offline := func() bool { var o bool; w.With(func() { o = dev.Offline }); return o }
	w.Tick(clock.Advance(30 * time.Second))
	if !offline() {
		t.Fatal("still inside the duration")
	}
	w.Tick(clock.Advance(31 * time.Second))
	if offline() {
		t.Fatal("scenario should have reverted itself")
	}
}

func TestScenarioValidation(t *testing.T) {
	devs := labDevices(t)
	w := NewWorld(devs, Options{Seed: 1})
	cases := map[string]Scenario{
		"unknown device":       {Device: "nope", Event: "offline"},
		"unsupported for type": {Device: "server-01", Event: "interface-down"},
		"ups only":             {Device: "switch-01", Event: "power-loss"},
		"unknown event":        {Device: "switch-01", Event: "explode"},
		"unknown interface":    {Device: "switch-01", Event: "interface-down", Args: map[string]string{"interface": "Gi9/9"}},
	}
	for name, s := range cases {
		if _, err := w.Apply(s); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := w.Apply(Scenario{Device: "switch-01", Event: "reset"}); err != nil {
		t.Fatal(err)
	}
}

func TestNoiseIsDeterministicPerSeed(t *testing.T) {
	devs := labDevices(t)
	run := func(seed int64) float64 {
		clock := &fakeClock{t: time.Unix(1_800_000_000, 0)}
		w := NewWorld(devs, Options{Seed: seed, Now: clock.Now})
		for i := 0; i < 50; i++ {
			w.Tick(clock.Advance(10 * time.Second))
		}
		d, _ := w.Device("server-01")
		var cpu float64
		w.With(func() { cpu = d.CPU })
		return cpu
	}
	if run(7) != run(7) {
		t.Fatal("same seed must give the same simulation")
	}
	if run(7) == run(8) {
		t.Fatal("different seeds should differ")
	}
}
