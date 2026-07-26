package pipeline

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/enrich"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/normalize"
	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/state"
	"github.com/alfscherer/infra-observer/internal/testutil"
	"github.com/alfscherer/infra-observer/migrations"
)

// The idempotency guarantees must hold on the real database, under real
// concurrency, not only against the in-memory double.
func TestPostgresIdempotencyUnderConcurrentDuplicateDelivery(t *testing.T) {
	pool := testutil.PostgresPool(t)
	ctx := context.Background()
	if _, err := persistence.Migrate(ctx, pool, migrations.FS); err != nil {
		t.Fatal(err)
	}
	store := persistence.NewPGStore(pool)
	devices := []domain.Device{{ID: "switch-01", Hostname: "sw", ManagementAddress: "x", DeviceType: domain.DeviceSwitch, Enabled: true,
		Collection: domain.CollectionSpec{Protocol: "snmp"}}}
	if err := store.UpsertDevices(ctx, devices); err != nil {
		t.Fatal(err)
	}
	norm, _ := normalize.LoadFile("../../configs/normalization.yaml")
	defs, _ := state.LoadFile("../../configs/states.yaml")
	p := &Processor{
		Validator: schema.NewValidator(), Normalizer: norm, Enricher: enrich.Enricher{Inventory: inventory.NewRegistry(devices)},
		States: defs, Store: store, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	if _, err := p.Process(ctx, ifObs(1, true)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Process(ctx, ifObs(2, false)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Process(ctx, ifObs(3, false)); err != nil {
				t.Errorf("process: %v", err)
			}
		}()
	}
	wg.Wait()

	count := func(q string) int {
		var n int
		if err := pool.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count(`SELECT count(*) FROM observations`); n != 3 {
		t.Errorf("observations = %d, want 3", n)
	}
	if n := count(`SELECT count(*) FROM events`); n != 1 {
		t.Errorf("events = %d, want exactly 1 despite 10 concurrent deliveries", n)
	}
	if n := count(`SELECT count(*) FROM alerts WHERE status='firing'`); n != 1 {
		t.Errorf("firing alerts = %d, want 1", n)
	}
	if n := count(`SELECT count(*) FROM outbox`); n != 2 {
		t.Errorf("outbox = %d, want 2 (events.device + events.alert)", n)
	}
	if n := count(`SELECT count(*) FROM state_transitions`); n != 3 {
		t.Errorf("transitions = %d, want 3 (adopt, suspect, down)", n)
	}

	// Two relays racing over one outbox must not publish anything twice.
	pub := &fakePublisher{}
	r := &Relay{Store: store, Publisher: pub, BatchSize: 1}
	var rg sync.WaitGroup
	for i := 0; i < 2; i++ {
		rg.Add(1)
		go func() { defer rg.Done(); _, _ = r.Drain(ctx) }()
	}
	rg.Wait()
	if len(pub.got) != 2 {
		t.Fatalf("outbox messages published %d times, want 2", len(pub.got))
	}
}
