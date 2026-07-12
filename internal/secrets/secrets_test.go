package secrets

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/alfscherer/infra-observer/internal/domain"
)

func TestResolveFromEnvThenFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "snmp"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "snmp", "file.json"), []byte(`{"community":"from-file"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"INFRA_OBSERVER_SECRET_SNMP_LAB": `{"community":"from-env"}`, "INFRA_OBSERVER_SECRET_TOKEN": "abc"}
	r := EnvFile{Dir: dir, Lookup: func(k string) (string, bool) { v, ok := env[k]; return v, ok }}
	ctx := context.Background()

	if s, err := r.Resolve(ctx, "snmp/lab"); err != nil || s.Get("community") != "from-env" {
		t.Fatalf("env: %v %v", s, err)
	}
	if s, err := r.Resolve(ctx, "snmp/file"); err != nil || s.Get("community") != "from-file" {
		t.Fatalf("file: %v %v", s, err)
	}
	if s, err := r.Resolve(ctx, "token"); err != nil || s.Get("value") != "abc" {
		t.Fatalf("bare: %v %v", s, err)
	}
	if _, err := r.Resolve(ctx, "missing"); domain.CategoryOf(err) != domain.CategoryAuthentication {
		t.Fatalf("missing ref should be an authentication error, got %v", err)
	}
}

func TestRefsCannotEscape(t *testing.T) {
	for _, ref := range []string{"../etc/passwd", "/abs", "a/../b", "", "a b"} {
		if ValidRef(ref) {
			t.Errorf("%q must be rejected", ref)
		}
	}
	if _, err := (EnvFile{Dir: t.TempDir()}).Resolve(context.Background(), "../x"); domain.CategoryOf(err) != domain.CategoryValidation {
		t.Fatalf("got %v", err)
	}
}

func TestSecretNeverPrints(t *testing.T) {
	s := Secret{"password": "hunter2"}
	if got := fmt.Sprint(s); got != "[redacted]" {
		t.Fatalf("Sprint leaked: %s", got)
	}
	if got := fmt.Sprintf("%v %s", s, s); got != "[redacted] [redacted]" {
		t.Fatalf("Sprintf leaked: %s", got)
	}
}
