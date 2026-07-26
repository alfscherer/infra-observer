package persistence

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// migrationLockID is an arbitrary constant used with pg_advisory_lock so that
// several processes starting at once apply migrations one at a time.
const migrationLockID = 7305192431

// Migrate applies every pending migration in lexical order, each in its own
// transaction, and records it in schema_migrations. It is safe to run
// concurrently and repeatedly. It returns the versions it applied.
func Migrate(ctx context.Context, pool *pgxpool.Pool, migrations fs.FS) ([]string, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, domain.Wrap(domain.CategoryDependency, "migrate: acquire connection", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return nil, domain.Wrap(domain.CategoryDependency, "migrate: lock", err)
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrationLockID) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, domain.Wrap(domain.CategoryDependency, "migrate: create schema_migrations", err)
	}
	done := map[string]bool{}
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, domain.Wrap(domain.CategoryDependency, "migrate: read versions", err)
	}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return nil, err
		}
		done[v] = true
	}
	rows.Close()

	names, err := fs.Glob(migrations, "*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	var applied []string
	for _, name := range names {
		version := strings.TrimSuffix(name, ".sql")
		if done[version] {
			continue
		}
		sqlText, err := fs.ReadFile(migrations, name)
		if err != nil {
			return applied, err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return applied, domain.Wrap(domain.CategoryDependency, "migrate: begin", err)
		}
		if _, err := tx.Exec(ctx, string(sqlText)); err != nil {
			_ = tx.Rollback(ctx)
			return applied, fmt.Errorf("migration %s failed (rolled back): %w", version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			_ = tx.Rollback(ctx)
			return applied, err
		}
		if err := tx.Commit(ctx); err != nil {
			return applied, err
		}
		applied = append(applied, version)
	}
	return applied, nil
}
