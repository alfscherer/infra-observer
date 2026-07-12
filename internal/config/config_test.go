package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func noEnv(string) (string, bool) { return "", false }

func TestDefaultsAreValidAndSafe(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}
	if !c.Automation.DefaultDryRun {
		t.Fatal("automation must default to dry-run")
	}
}

func TestFileOverridesDefaults(t *testing.T) {
	c, err := load(write(t, "processing:\n  workers: 3\ncollectors:\n  snmp:\n    timeout: 5s\n"), noEnv)
	if err != nil {
		t.Fatal(err)
	}
	if c.Processing.Workers != 3 || c.Collectors.SNMP.Timeout != 5*time.Second {
		t.Fatalf("overrides not applied: %+v", c.Processing)
	}
	if c.Processing.QueueSize != Default().Processing.QueueSize {
		t.Fatal("unspecified fields must keep defaults")
	}
}

func TestEnvOverridesFile(t *testing.T) {
	env := map[string]string{
		"INFRA_OBSERVER_PROCESSING_WORKERS":          "12",
		"INFRA_OBSERVER_SCRIPTING_EXECUTION_TIMEOUT": "100ms",
		"INFRA_OBSERVER_AUTOMATION_DEFAULT_DRY_RUN":  "false",
		"INFRA_OBSERVER_SCRIPTING_DIRECTORIES":       "a, b",
	}
	c, err := load(write(t, "processing:\n  workers: 3\n"), func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	if c.Processing.Workers != 12 || c.Scripting.ExecutionTimeout != 100*time.Millisecond || c.Automation.DefaultDryRun {
		t.Fatalf("env not applied: %+v", c)
	}
	if len(c.Scripting.Directories) != 2 || c.Scripting.Directories[1] != "b" {
		t.Fatalf("slice env: %v", c.Scripting.Directories)
	}
}

func TestInvalidConfigurationFailsFast(t *testing.T) {
	cases := map[string]string{
		"unknown key":      "processing:\n  wrkers: 3\n",
		"non positive":     "processing:\n  workers: 0\n",
		"bad nats":         "nats:\n  url: http://x\n",
		"huge js deadline": "scripting:\n  execution_timeout: 1m\n",
		"bad page sizes":   "api:\n  default_page_size: 900\n",
		"bad endpoint":     "scripting:\n  endpoints:\n    x: {timeout: 1s}\n",
	}
	for name, body := range cases {
		if _, err := load(write(t, body), noEnv); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if _, err := load("", func(k string) (string, bool) {
		if k == "INFRA_OBSERVER_PROCESSING_WORKERS" {
			return "many", true
		}
		return "", false
	}); err == nil || !strings.Contains(err.Error(), "INFRA_OBSERVER_PROCESSING_WORKERS") {
		t.Fatalf("bad env value must name the variable, got %v", err)
	}
}

func TestRequireDatabase(t *testing.T) {
	if err := Default().RequireDatabase(); err == nil {
		t.Fatal("empty database url must be rejected for roles that need it")
	}
}

func TestShippedConfigLoads(t *testing.T) {
	if _, err := load("../../configs/config.yaml", noEnv); err != nil {
		t.Fatal(err)
	}
}

func TestScriptSettingsDefaultEnabled(t *testing.T) {
	if !(ScriptSettings{}).IsEnabled() {
		t.Fatal("unmentioned script settings must default to enabled")
	}
	f := false
	if (ScriptSettings{Enabled: &f}).IsEnabled() {
		t.Fatal("explicit disable must win")
	}
}
