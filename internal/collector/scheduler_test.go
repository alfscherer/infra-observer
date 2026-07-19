package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/collector/snmp"
	"github.com/alfscherer/infra-observer/internal/domain"
)

type captureSink struct {
	mu   sync.Mutex
	obs  []domain.Observation
	inv  []InventoryFact
	fail error
}

func (c *captureSink) Observations(_ context.Context, o []domain.Observation) error {
	if c.fail != nil {
		return c.fail
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.obs = append(c.obs, o...)
	return nil
}

func (c *captureSink) Inventory(_ context.Context, f InventoryFact) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inv = append(c.inv, f)
	return nil
}

func (c *captureSink) metric(m string) []domain.Observation {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []domain.Observation
	for _, o := range c.obs {
		if o.Metric == m {
			out = append(out, o)
		}
	}
	return out
}

type fakePoller struct {
	fn func(ctx context.Context, dev domain.Device, cycle string) (snmp.Result, error)
}

func (f fakePoller) Poll(ctx context.Context, dev domain.Device, cycle string) (snmp.Result, error) {
	return f.fn(ctx, dev, cycle)
}

func dev(id string, iv time.Duration) domain.Device {
	return domain.Device{ID: id, Hostname: id, ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Enabled: true,
		Collection: domain.CollectionSpec{Protocol: "snmp", Interval: iv}}
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPollOncePublishesSuccessWithCorrelation(t *testing.T) {
	sink := &captureSink{}
	s := &Scheduler{Sink: sink, Log: quiet(), DefaultInterval: time.Second, Poller: fakePoller{func(_ context.Context, d domain.Device, cycle string) (snmp.Result, error) {
		return snmp.Result{
			Observations: []domain.Observation{{ObservationID: "o1", CorrelationID: cycle, DeviceID: d.ID, Source: "snmp", Metric: "snmp.sysName", Value: "x"}},
			Facts:        snmp.Facts{SysName: "sw", ProfileID: "p"},
		}, nil
	}}}
	s.PollOnce(context.Background(), dev("sw1", 0))
	ok := sink.metric(MetricPollSuccess)
	if len(ok) != 1 || ok[0].Value != true {
		t.Fatalf("expected one successful poll marker: %+v", ok)
	}
	if len(sink.metric("snmp.sysName")) != 1 || len(sink.metric(MetricPollDuration)) != 1 {
		t.Fatal("device observations and duration must be published")
	}
	if ok[0].CorrelationID == "" || ok[0].CorrelationID != sink.metric("snmp.sysName")[0].CorrelationID {
		t.Fatal("one cycle must share one correlation id")
	}
	if len(sink.inv) != 1 || sink.inv[0].SysName != "sw" {
		t.Fatalf("inventory fact missing: %+v", sink.inv)
	}
}

func TestPollOnceReportsFailureAsObservationOnly(t *testing.T) {
	sink := &captureSink{}
	var hooked atomic.Int32
	s := &Scheduler{Sink: sink, Log: quiet(), DefaultInterval: time.Second,
		OnPoll: func(e PollEvent) {
			if e.Err != nil {
				hooked.Add(1)
			}
		},
		Poller: fakePoller{func(context.Context, domain.Device, string) (snmp.Result, error) {
			// partial data must not leak out of a failed poll
			return snmp.Result{Observations: []domain.Observation{{Metric: "partial"}}}, domain.Errorf(domain.CategoryTimeout, "no response")
		}}}
	s.PollOnce(context.Background(), dev("sw1", 0))
	ok := sink.metric(MetricPollSuccess)
	if len(ok) != 1 || ok[0].Value != false {
		t.Fatalf("failure must be published as poll_success=false: %+v", ok)
	}
	if ok[0].Metadata["error_category"] != "timeout" {
		t.Fatalf("error category missing: %+v", ok[0].Metadata)
	}
	if len(sink.metric("partial")) != 0 || len(sink.inv) != 0 {
		t.Fatal("a failed poll must not publish partial data or inventory")
	}
	if hooked.Load() != 1 {
		t.Fatal("OnPoll hook not invoked")
	}
}

func TestPollOnceSurvivesSinkFailure(t *testing.T) {
	sink := &captureSink{fail: errors.New("nats down")}
	s := &Scheduler{Sink: sink, Log: quiet(), DefaultInterval: time.Second,
		Poller: fakePoller{func(context.Context, domain.Device, string) (snmp.Result, error) { return snmp.Result{}, nil }}}
	s.PollOnce(context.Background(), dev("sw1", 0)) // must not panic or block
}

func TestRunRespectsIntervalsAndWorkerBound(t *testing.T) {
	sink := &captureSink{}
	var running, peak atomic.Int32
	perDevice := map[string]*atomic.Int32{}
	var mu sync.Mutex
	devices := []domain.Device{}
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		devices = append(devices, dev(id, 40*time.Millisecond))
		perDevice[id] = &atomic.Int32{}
	}
	s := &Scheduler{
		Devices: func() []domain.Device { return devices }, Sink: sink, Log: quiet(), Workers: 2, DefaultInterval: 40 * time.Millisecond,
		Poller: fakePoller{func(_ context.Context, d domain.Device, _ string) (snmp.Result, error) {
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			mu.Lock()
			c := perDevice[d.ID]
			mu.Unlock()
			if c.Add(1) > 1 {
				t.Errorf("device %s polled concurrently with itself", d.ID)
			}
			time.Sleep(15 * time.Millisecond)
			c.Add(-1)
			running.Add(-1)
			return snmp.Result{}, nil
		}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if peak.Load() > 2 {
		t.Fatalf("concurrency %d exceeded worker bound 2", peak.Load())
	}
	byDev := map[string]int{}
	for _, o := range sink.metric(MetricPollSuccess) {
		byDev[o.DeviceID]++
	}
	for _, d := range devices {
		if byDev[d.ID] < 2 {
			t.Errorf("device %s polled %d times, want >= 2", d.ID, byDev[d.ID])
		}
	}
}

func TestRunSkipsDisabledAndNonSNMPDevices(t *testing.T) {
	sink := &captureSink{}
	off := dev("off", 20*time.Millisecond)
	off.Enabled = false
	script := dev("script", 20*time.Millisecond)
	script.Collection.Protocol = "script"
	s := &Scheduler{Devices: func() []domain.Device { return []domain.Device{off, script, dev("on", 20*time.Millisecond)} },
		Sink: sink, Log: quiet(), Workers: 2, DefaultInterval: time.Second,
		Poller: fakePoller{func(context.Context, domain.Device, string) (snmp.Result, error) { return snmp.Result{}, nil }}}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_ = s.Run(ctx)
	for _, o := range sink.metric(MetricPollSuccess) {
		if o.DeviceID != "on" {
			t.Fatalf("polled %s", o.DeviceID)
		}
	}
	if len(sink.metric(MetricPollSuccess)) == 0 {
		t.Fatal("enabled device never polled")
	}
}

func TestOffsetIsDeterministicAndBounded(t *testing.T) {
	iv := 30 * time.Second
	if offset("x", iv) != offset("x", iv) {
		t.Fatal("offset must be deterministic")
	}
	for _, id := range []string{"a", "b", "switch-01", "ups-01"} {
		if o := offset(id, iv); o < 0 || o >= iv {
			t.Fatalf("offset %v out of range", o)
		}
	}
}
