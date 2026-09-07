// Package e2e runs the whole platform in one process against real
// infrastructure: an embedded JetStream-enabled NATS server, a real PostgreSQL
// (schema-isolated per test), real UDP SNMP agents (the simulator), the real
// collector, the real processing and automation roles assembled by internal/app,
// and the shipped configuration, profiles and JavaScript extensions.
//
// Tests skip when no database is configured (INFRA_OBSERVER_TEST_DATABASE_URL);
// `make e2e` provides one.
package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/alfscherer/infra-observer/internal/app"
	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/automation/adapters"
	"github.com/alfscherer/infra-observer/internal/collector"
	"github.com/alfscherer/infra-observer/internal/collector/snmp"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/enrich"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/normalize"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/pipeline"
	"github.com/alfscherer/infra-observer/internal/rules"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/secrets"
	"github.com/alfscherer/infra-observer/internal/sim"
	"github.com/alfscherer/infra-observer/internal/state"
	"github.com/alfscherer/infra-observer/internal/testutil"
	"github.com/alfscherer/infra-observer/migrations"
)

var t0 = time.Now().UTC()

type labOpts struct {
	maxDeliver   int
	retryDelay   time.Duration
	extraScripts string // directory with additional script kinds (fault injection)
	policies     string
	dryRun       bool
}

// lab is one complete platform instance.
type lab struct {
	t       *testing.T
	nats    *testutil.NATS
	client  *messaging.Client
	pg      *persistence.PGStore
	world   *sim.World
	devices []domain.Device
	reg     *inventory.Registry
	cfg     config.Config
	log     *slog.Logger
	svc     *scripting.Service
	ext     *scripting.Extensions
	opts    labOpts
	reader  *adapters.Reader
}

func quietLog() *slog.Logger {
	if os.Getenv("E2E_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newLab(t *testing.T, o labOpts) *lab {
	t.Helper()
	pool := testutil.PostgresPool(t) // skips without a database
	ctx := context.Background()
	if _, err := persistence.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatal(err)
	}
	l := &lab{t: t, pg: persistence.NewPGStore(pool), log: quietLog(), opts: o}
	l.nats = testutil.StartNATS(t)
	c, err := messaging.Connect(l.nats.URL(), "e2e", l.log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	l.client = c
	if err := c.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}

	devs, err := inventory.LoadFile("../../configs/inventory.yaml")
	if err != nil {
		t.Fatal(err)
	}
	l.world = sim.NewWorld(devs, sim.Options{Seed: 21})
	actx, acancel := context.WithCancel(context.Background())
	t.Cleanup(acancel)
	for i := range devs {
		a, err := l.world.Listen(actx, devs[i].ID, "127.0.0.1:0", l.log)
		if err != nil {
			t.Fatal(err)
		}
		devs[i].ManagementAddress = a.Addr().String()
		devs[i].Collection.Interval = 150 * time.Millisecond
	}
	l.devices = devs
	l.reg = inventory.NewRegistry(devs)
	if err := l.pg.UpsertDevices(ctx, devs); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Processing.Workers, cfg.Processing.QueueSize = 4, 8
	cfg.Processing.MaxDeliver, cfg.Processing.RetryDelay, cfg.Processing.AckWait = 6, 100*time.Millisecond, 3*time.Second
	if o.maxDeliver > 0 {
		cfg.Processing.MaxDeliver = o.maxDeliver
	}
	if o.retryDelay > 0 {
		cfg.Processing.RetryDelay = o.retryDelay
	}
	cfg.Processing.StaleAfter, cfg.Processing.SweepInterval = time.Hour, time.Hour
	cfg.Automation.TickInterval = 100 * time.Millisecond
	cfg.Scripting.QuarantineAfter = 4
	cfg.Scripting.Directories = []string{"../../scripts"}
	if o.extraScripts != "" {
		cfg.Scripting.Directories = append(cfg.Scripting.Directories, o.extraScripts)
	}
	l.cfg = cfg

	full, err := config.Load("../../configs/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	svc, rep := scripting.New(cfg.Scripting, full.Scripts, []runtime.Installer{api.Installer(api.Deps{Inventory: l.reg, State: l.pg, Log: l.log})}, l.log)
	t.Cleanup(svc.Close)
	if rep.Loaded < 5 {
		t.Fatalf("scripts: %+v", rep)
	}
	l.svc = svc
	l.ext = &scripting.Extensions{Svc: svc, Validator: schema.NewValidator(), Log: l.log}
	l.reader = &adapters.Reader{
		Dialer:  snmp.GoSNMPDialer{Timeout: 200 * time.Millisecond, MaxRepetitions: 10},
		Secrets: secrets.Static{"snmp/lab": {"version": "2c", "community": "lab-ro"}},
	}
	return l
}

// processor assembles the processing role over store (which may be a fault-injecting wrapper).
func (l *lab) processor(store persistence.Store, suffix string) *app.Processor {
	norm, err := normalize.LoadFile("../../configs/normalization.yaml")
	if err != nil {
		l.t.Fatal(err)
	}
	defs, _ := state.LoadFile("../../configs/states.yaml")
	rs, _ := rules.LoadFile("../../configs/rules.yaml")
	proc := &pipeline.Processor{
		Validator: schema.NewValidator(), Normalizer: norm, Enricher: enrich.Enricher{Inventory: l.reg},
		States: defs, Rules: rs, Store: store, Ext: l.ext, Log: l.log, StaleAfter: time.Hour, SweepInterval: time.Hour, StartedAt: time.Now(),
	}
	return &app.Processor{Cfg: l.cfg, Log: l.log, Client: l.client, Store: store, Registry: l.reg, Proc: proc, DurableSuffix: suffix}
}

// automationRole assembles the automation role with the world as the device controller.
func (l *lab) automationRole(policies string, dryRun bool) *app.Automation {
	pols, err := automation.Parse([]byte(policies))
	if err != nil {
		l.t.Fatal(err)
	}
	generic := &adapters.Generic{Record: func(ctx context.Context, ev domain.Event) error { return pipeline.PersistEvent(ctx, l.pg, ev) }}
	engine := &automation.Engine{
		Policies: pols, Store: l.pg, Devices: l.reg, Proposer: l.ext, GlobalDryRun: dryRun, Log: l.log,
		Adapters: adapters.NewSet(sim.DeviceController{C: l.world.Control()}, l.reader, generic),
	}
	handler := &automation.Worker{Engine: engine, Devices: l.reg, Log: l.log,
		Integrations: scripting.IntegrationRunner{Ext: l.ext, Log: l.log, Persist: func(ctx context.Context, ev domain.Event) error { return pipeline.PersistEvent(ctx, l.pg, ev) }}}
	return &app.Automation{Cfg: l.cfg, Log: l.log, Client: l.client, Store: l.pg, Engine: engine, Handler: handler}
}

// collectorLoop runs the real collector against the simulator.
func (l *lab) collectorLoop(ctx context.Context, onPoll func(collector.PollEvent)) {
	profiles, err := snmp.LoadProfiles("../../configs/profiles")
	if err != nil {
		l.t.Fatal(err)
	}
	sched := &collector.Scheduler{
		Devices: l.reg.List, DefaultInterval: time.Second, Workers: 6, Log: l.log, OnPoll: onPoll,
		Sink: collector.NATSSink{Client: l.client},
		Poller: &snmp.Poller{
			Dialer:   snmp.GoSNMPDialer{Timeout: 150 * time.Millisecond, MaxRepetitions: 10},
			Profiles: profiles, Secrets: secrets.Static{"snmp/lab": {"version": "2c", "community": "lab-ro"}},
		},
	}
	go func() { _ = sched.Run(ctx) }()
	go l.world.RunClock(ctx, 100*time.Millisecond)
}

// run starts f in the background and returns a stop function that waits for it.
func run(t *testing.T, f func(ctx context.Context) error) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := f(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("role stopped: %v", err)
		}
	}()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// --- observation helpers ---------------------------------------------------------

func (l *lab) events(store persistence.Reader, device, typ string) []domain.Event {
	items, _, err := store.ListEvents(context.Background(), persistence.EventFilter{DeviceID: device, Type: typ}, persistence.Page{Limit: 500})
	if err != nil {
		l.t.Fatal(err)
	}
	return items
}

func (l *lab) countRows(q string, args ...any) int {
	l.t.Helper()
	return countRows(l.t, l.pg, q, args...)
}

func (l *lab) deadLetters() []messaging.DeadLetterRecord {
	l.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	recs, err := l.client.ListDeadLetters(ctx, 1000)
	if err != nil {
		l.t.Fatal(err)
	}
	return recs
}

var rawSeq atomic.Int64

// publishRaw puts an observation on the raw subject with a fresh JetStream
// message id, so publish-side de-duplication cannot hide consumer behaviour.
func (l *lab) publishRaw(payload []byte) {
	l.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := fmt.Sprintf("raw-%d-%d", time.Now().UnixNano(), rawSeq.Add(1))
	if err := l.client.Publish(ctx, messaging.SubjectRawSNMP, id, payload, map[string]string{messaging.HeaderSchema: schema.ObservationV1}); err != nil {
		l.t.Fatal(err)
	}
}

func rawObs(id, device, metric string, value any, at time.Time) []byte {
	b, _ := schema.Encode(domain.Observation{ObservationID: id, CorrelationID: "corr-" + id, DeviceID: device, Source: "snmp", Metric: metric, Value: value, ObservedAt: at})
	return b
}

// faultyStore fails database operations on demand, to model an outage without
// stopping the real server.
type faultyStore struct {
	*persistence.PGStore
	down atomic.Bool
	hits atomic.Int64
}

func (f *faultyStore) fail() error {
	if f.down.Load() {
		f.hits.Add(1)
		return domain.Errorf(domain.CategoryDependency, "connection refused (injected outage)")
	}
	return nil
}
func (f *faultyStore) Do(ctx context.Context, fn func(persistence.Tx) error) error {
	if err := f.fail(); err != nil {
		return err
	}
	return f.PGStore.Do(ctx, fn)
}
func (f *faultyStore) DrainOutbox(ctx context.Context, n int, pub func(context.Context, []persistence.OutboxMessage) error) (int, error) {
	if err := f.fail(); err != nil {
		return 0, err
	}
	return f.PGStore.DrainOutbox(ctx, n, pub)
}
func (f *faultyStore) ListStates(ctx context.Context) ([]domain.StateRecord, error) {
	if err := f.fail(); err != nil {
		return nil, err
	}
	return f.PGStore.ListStates(ctx)
}

func writeScript(t *testing.T, dir, kind, name, src string) {
	t.Helper()
	p := filepath.Join(dir, kind, name+".js")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
}

// streamSubjects asks for per-subject message counts in stream info.
func streamSubjects() jetstream.StreamInfoOpt { return jetstream.WithSubjectFilter(">") }
