// Package secrets resolves credential references to secret values.
//
// Devices, automation policies and script endpoints only ever hold a reference
// such as "snmp/lab-v3". The value is looked up here, at the moment of use, by
// the component that needs it. Nothing resolves secrets on behalf of
// JavaScript: scripts can name an endpoint, never read its credentials.
//
// Local development resolves from environment variables and JSON files. A
// production deployment would implement Resolver against Vault, a cloud secret
// manager or systemd credentials (see OPERATIONS.md); no caller would change.
package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/alfscherer/infra-observer/internal/domain"
)

// Secret is a bag of named values. It never prints its contents.
type Secret map[string]string

func (s Secret) String() string { return "[redacted]" }

// LogValue keeps secrets out of structured logs even if logged by mistake.
func (s Secret) LogValue() slog.Value { return slog.StringValue("[redacted]") }

// Get returns a value, or "" when absent.
func (s Secret) Get(key string) string { return s[key] }

// Resolver turns a reference into a secret.
type Resolver interface {
	Resolve(ctx context.Context, ref string) (Secret, error)
}

var refPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]*$`)

// ValidRef reports whether ref is well formed and cannot escape a directory.
func ValidRef(ref string) bool {
	return refPattern.MatchString(ref) && !strings.Contains(ref, "..")
}

// EnvFile resolves "a/b" from INFRA_OBSERVER_SECRET_A_B (a JSON object, or a
// bare string stored under the key "value"), then from <Dir>/a/b.json.
type EnvFile struct {
	Dir    string
	Lookup func(string) (string, bool) // defaults to os.LookupEnv
}

func (r EnvFile) Resolve(_ context.Context, ref string) (Secret, error) {
	if !ValidRef(ref) {
		return nil, domain.Errorf(domain.CategoryValidation, "invalid credential reference %q", ref)
	}
	lookup := r.Lookup
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if raw, ok := lookup(envName(ref)); ok {
		return parse(raw, "environment "+envName(ref))
	}
	if r.Dir != "" {
		b, err := os.ReadFile(filepath.Join(r.Dir, filepath.FromSlash(ref)+".json"))
		if err == nil {
			return parse(string(b), "file for "+ref)
		}
		if !os.IsNotExist(err) {
			return nil, domain.Wrap(domain.CategoryDependency, "read secret file", err)
		}
	}
	return nil, domain.Errorf(domain.CategoryAuthentication, "credential reference %q not found", ref)
}

func envName(ref string) string {
	var sb strings.Builder
	sb.WriteString("INFRA_OBSERVER_SECRET_")
	for _, c := range strings.ToUpper(ref) {
		if (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			sb.WriteRune(c)
		} else {
			sb.WriteByte('_')
		}
	}
	return sb.String()
}

func parse(raw, origin string) (Secret, error) {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "{") {
		var s Secret
		if err := json.Unmarshal([]byte(raw), &s); err != nil {
			return nil, domain.Errorf(domain.CategoryValidation, "%s: not a JSON object of strings: %v", origin, err)
		}
		return s, nil
	}
	if raw == "" {
		return nil, domain.Errorf(domain.CategoryValidation, "%s is empty", origin)
	}
	return Secret{"value": raw}, nil
}

// Static is an in-memory Resolver for tests and simulations.
type Static map[string]Secret

func (s Static) Resolve(_ context.Context, ref string) (Secret, error) {
	if v, ok := s[ref]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("%w", domain.Errorf(domain.CategoryAuthentication, "credential reference %q not found", ref))
}
