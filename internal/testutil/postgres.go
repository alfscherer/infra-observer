package testutil

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DatabaseEnv names the environment variable holding a PostgreSQL URL for
// integration tests. When unset those tests are skipped, so `make test` needs
// no database; `make integration-test` provides one.
const DatabaseEnv = "INFRA_OBSERVER_TEST_DATABASE_URL"

var schemaSeq atomic.Int64

// PostgresPool returns a pool confined to a fresh, uniquely named schema that
// is dropped when the test ends. Tests therefore never see each other's data
// and can run in parallel against one server.
func PostgresPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv(DatabaseEnv)
	if url == "" {
		t.Skipf("%s not set; skipping PostgreSQL test", DatabaseEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	schema := fmt.Sprintf("t_%d_%d", time.Now().UnixNano()%1e9, schemaSeq.Add(1))
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 10
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		admin.Close()
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = admin.Exec(c, "DROP SCHEMA "+strings.ToLower(schema)+" CASCADE")
		admin.Close()
	})
	return pool
}
