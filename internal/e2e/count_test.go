package e2e

import (
	"context"
	"testing"

	"github.com/alfscherer/infra-observer/internal/persistence"
)

// countRows runs a COUNT query against the lab database. The PGStore does not
// expose its pool, so tests reach it through the observer's own connection.
func countRows(t *testing.T, s *persistence.PGStore, q string, args ...any) int {
	t.Helper()
	n, err := s.CountForTest(context.Background(), q, args...)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
