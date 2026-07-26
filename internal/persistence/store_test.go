package persistence_test

import (
	"context"
	"testing"

	"github.com/alfscherer/infra-observer/internal/persistence"
	"github.com/alfscherer/infra-observer/internal/persistence/storetest"
	"github.com/alfscherer/infra-observer/internal/testutil"
	"github.com/alfscherer/infra-observer/migrations"
)

func TestMemStoreContract(t *testing.T) {
	storetest.Run(t, func(*testing.T) persistence.Store { return persistence.NewMemStore() })
}

func newPG(t *testing.T) persistence.Store {
	t.Helper()
	pool := testutil.PostgresPool(t)
	if _, err := persistence.Migrate(context.Background(), pool, migrations.FS); err != nil {
		t.Fatal(err)
	}
	return persistence.NewPGStore(pool)
}

func TestPGStoreContract(t *testing.T) { storetest.Run(t, newPG) }

func TestMigrateIsIdempotentAndOrdered(t *testing.T) {
	pool := testutil.PostgresPool(t)
	ctx := context.Background()
	first, err := persistence.Migrate(ctx, pool, migrations.FS)
	if err != nil || len(first) == 0 || first[0] != "0001_core" {
		t.Fatalf("first run: %v %v", first, err)
	}
	second, err := persistence.Migrate(ctx, pool, migrations.FS)
	if err != nil || len(second) != 0 {
		t.Fatalf("second run must apply nothing: %v %v", second, err)
	}
}
