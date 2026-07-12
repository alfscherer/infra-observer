// Package config loads the YAML configuration file, applies environment
// overrides and validates the result. Invalid configuration fails startup
// instead of surfacing later as a confusing runtime error.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// EnvPrefix namespaces every environment override, e.g.
// INFRA_OBSERVER_NATS_URL or INFRA_OBSERVER_PROCESSING_WORKERS.
const EnvPrefix = "INFRA_OBSERVER"

type Config struct {
	Logging    Logging    `yaml:"logging"`
	NATS       NATS       `yaml:"nats"`
	Database   Database   `yaml:"database"`
	Inventory  Inventory  `yaml:"inventory"`
	Collectors Collectors `yaml:"collectors"`
	Processing Processing `yaml:"processing"`
	Scripting  Scripting  `yaml:"scripting"`
	Automation Automation `yaml:"automation"`
	API        API        `yaml:"api"`
	Secrets    Secrets    `yaml:"secrets"`
	Simulator  Simulator  `yaml:"simulator"`

	// Scripts holds per-script settings keyed by kind then script id, kept apart
	// from script source so secrets and endpoints never live in JavaScript.
	Scripts map[string]map[string]ScriptSettings `yaml:"scripts"`
}

type Logging struct {
	Level  string `yaml:"level"`  // debug, info, warn, error
	Format string `yaml:"format"` // json or text
}

type NATS struct {
	URL string `yaml:"url"`
}

type Database struct {
	URL      string `yaml:"url"`
	MaxConns int    `yaml:"max_conns"`
}

type Inventory struct {
	File string `yaml:"file"`
}

type Collectors struct {
	SNMP SNMP `yaml:"snmp"`
}

type SNMP struct {
	Workers         int           `yaml:"workers"`
	Timeout         time.Duration `yaml:"timeout"`
	Retries         int           `yaml:"retries"`
	DefaultInterval time.Duration `yaml:"default_interval"`
	ProfilesDir     string        `yaml:"profiles_dir"`
	MaxRepetitions  int           `yaml:"max_repetitions"`
}

type Processing struct {
	Workers           int           `yaml:"workers"`        // shards; ordering is per device within a shard
	QueueSize         int           `yaml:"queue_size"`     // per-shard buffer; the backpressure bound
	MaxDeliver        int           `yaml:"max_deliver"`    // deliveries before a message is dead-lettered
	AckWait           time.Duration `yaml:"ack_wait"`       // JetStream redelivery deadline
	RetryDelay        time.Duration `yaml:"retry_delay"`    // base NAK delay, grows per attempt
	StaleAfter        time.Duration `yaml:"stale_after"`    // no data for this long counts as a failed sample
	SweepInterval     time.Duration `yaml:"sweep_interval"` // how often staleness is evaluated
	NormalizationFile string        `yaml:"normalization_file"`
	StatesFile        string        `yaml:"states_file"`
	RulesFile         string        `yaml:"rules_file"`
}

type Scripting struct {
	Enabled          bool                `yaml:"enabled"`
	Directories      []string            `yaml:"directories"`
	Workers          int                 `yaml:"workers"`           // one runtime per worker
	QueueSize        int                 `yaml:"queue_size"`        // pending invocations before callers see saturation
	ExecutionTimeout time.Duration       `yaml:"execution_timeout"` // per-invocation deadline
	QuarantineAfter  int                 `yaml:"quarantine_after"`  // consecutive failures before a script is disabled
	Endpoints        map[string]Endpoint `yaml:"endpoints"`         // named targets for http.request
}

// Endpoint is a named HTTP target scripts may call. The URL is a secret
// reference, never a literal, so scripts cannot name arbitrary hosts.
type Endpoint struct {
	URLRef  string        `yaml:"url_ref"`
	Timeout time.Duration `yaml:"timeout"`
}

// ScriptSettings configures one script independently of its source.
type ScriptSettings struct {
	Enabled *bool          `yaml:"enabled"`
	Config  map[string]any `yaml:"config"`
}

// IsEnabled defaults to true when the script is not mentioned.
func (s ScriptSettings) IsEnabled() bool { return s.Enabled == nil || *s.Enabled }

type Automation struct {
	DefaultDryRun bool          `yaml:"default_dry_run"`
	PoliciesFile  string        `yaml:"policies_file"`
	TickInterval  time.Duration `yaml:"tick_interval"`
	Workers       int           `yaml:"workers"`
}

type API struct {
	Listen          string `yaml:"listen"`
	DefaultPageSize int    `yaml:"default_page_size"`
	MaxPageSize     int    `yaml:"max_page_size"`
}

type Secrets struct {
	Dir string `yaml:"dir"`
}

type Simulator struct {
	BindHost string `yaml:"bind_host"`
	Seed     int64  `yaml:"seed"`
}

// Default returns the baseline configuration. Safe defaults matter: automation
// is dry-run unless someone explicitly says otherwise.
func Default() Config {
	return Config{
		Logging:   Logging{Level: "info", Format: "json"},
		NATS:      NATS{URL: "nats://127.0.0.1:4222"},
		Database:  Database{MaxConns: 8},
		Inventory: Inventory{File: "configs/inventory.yaml"},
		Collectors: Collectors{SNMP: SNMP{
			Workers: 20, Timeout: 2 * time.Second, Retries: 2,
			DefaultInterval: 30 * time.Second, ProfilesDir: "configs/profiles", MaxRepetitions: 20,
		}},
		Processing: Processing{
			Workers: 8, QueueSize: 64, MaxDeliver: 5, AckWait: 30 * time.Second,
			RetryDelay: 2 * time.Second, StaleAfter: 2 * time.Minute, SweepInterval: 30 * time.Second,
			NormalizationFile: "configs/normalization.yaml",
			StatesFile:        "configs/states.yaml",
			RulesFile:         "configs/rules.yaml",
		},
		Scripting: Scripting{
			Enabled: true, Directories: []string{"./scripts"}, Workers: 4, QueueSize: 128,
			ExecutionTimeout: 250 * time.Millisecond, QuarantineAfter: 5,
		},
		Automation: Automation{DefaultDryRun: true, PoliciesFile: "configs/automation.yaml", TickInterval: 5 * time.Second, Workers: 4},
		API:        API{Listen: ":8080", DefaultPageSize: 50, MaxPageSize: 500},
		Secrets:    Secrets{Dir: "secrets"},
		Simulator:  Simulator{BindHost: "0.0.0.0", Seed: 1},
	}
}

// Load reads path (which may be empty to use defaults only), applies
// environment overrides and validates.
func Load(path string) (Config, error) { return load(path, os.LookupEnv) }

func load(path string, lookup func(string) (string, bool)) (Config, error) {
	cfg := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("read config: %w", err)
		}
		dec := yaml.NewDecoder(bytes.NewReader(raw))
		dec.KnownFields(true) // a typo in a key is an error, not a silently ignored setting
		if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
			return cfg, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	if err := applyEnv(reflect.ValueOf(&cfg).Elem(), EnvPrefix, lookup); err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

// Validate checks role-independent invariants.
func (c Config) Validate() error {
	var p []string
	add := func(cond bool, msg string) {
		if !cond {
			p = append(p, msg)
		}
	}
	add(oneOf(c.Logging.Level, "debug", "info", "warn", "error"), "logging.level must be debug, info, warn or error")
	add(oneOf(c.Logging.Format, "json", "text"), "logging.format must be json or text")
	add(strings.HasPrefix(c.NATS.URL, "nats://") || strings.HasPrefix(c.NATS.URL, "tls://"), "nats.url must start with nats:// or tls://")
	add(c.Database.MaxConns > 0, "database.max_conns must be positive")
	s := c.Collectors.SNMP
	add(s.Workers > 0, "collectors.snmp.workers must be positive")
	add(s.Timeout > 0, "collectors.snmp.timeout must be positive")
	add(s.Retries >= 0, "collectors.snmp.retries must not be negative")
	add(s.DefaultInterval > 0, "collectors.snmp.default_interval must be positive")
	add(s.MaxRepetitions > 0, "collectors.snmp.max_repetitions must be positive")
	pr := c.Processing
	add(pr.Workers > 0, "processing.workers must be positive")
	add(pr.QueueSize > 0, "processing.queue_size must be positive")
	add(pr.MaxDeliver > 0, "processing.max_deliver must be positive")
	add(pr.AckWait > 0, "processing.ack_wait must be positive")
	add(pr.RetryDelay > 0, "processing.retry_delay must be positive")
	add(pr.StaleAfter > 0, "processing.stale_after must be positive")
	add(pr.SweepInterval > 0, "processing.sweep_interval must be positive")
	sc := c.Scripting
	add(sc.Workers > 0, "scripting.workers must be positive")
	add(sc.QueueSize > 0, "scripting.queue_size must be positive")
	add(sc.ExecutionTimeout > 0, "scripting.execution_timeout must be positive")
	add(sc.ExecutionTimeout <= 10*time.Second, "scripting.execution_timeout above 10s defeats the purpose of a deadline")
	add(sc.QuarantineAfter > 0, "scripting.quarantine_after must be positive")
	for name, ep := range sc.Endpoints {
		add(ep.URLRef != "", fmt.Sprintf("scripting.endpoints.%s.url_ref is required", name))
	}
	a := c.Automation
	add(a.TickInterval > 0, "automation.tick_interval must be positive")
	add(a.Workers > 0, "automation.workers must be positive")
	add(c.API.DefaultPageSize > 0 && c.API.DefaultPageSize <= c.API.MaxPageSize, "api page sizes must satisfy 0 < default_page_size <= max_page_size")
	if len(p) > 0 {
		return fmt.Errorf("invalid configuration:\n  - %s", strings.Join(p, "\n  - "))
	}
	return nil
}

// RequireDatabase is called by roles that need PostgreSQL.
func (c Config) RequireDatabase() error {
	if c.Database.URL == "" {
		return errors.New("database.url is required (set it in the config file or INFRA_OBSERVER_DATABASE_URL)")
	}
	return nil
}

func oneOf(v string, options ...string) bool {
	for _, o := range options {
		if v == o {
			return true
		}
	}
	return false
}

var (
	durationType = reflect.TypeOf(time.Duration(0))
)

// applyEnv overrides scalar fields from environment variables derived from the
// yaml path, e.g. collectors.snmp.workers -> INFRA_OBSERVER_COLLECTORS_SNMP_WORKERS.
func applyEnv(v reflect.Value, prefix string, lookup func(string) (string, bool)) error {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		key := strings.Split(f.Tag.Get("yaml"), ",")[0]
		if key == "" || key == "-" {
			continue
		}
		name := prefix + "_" + strings.ToUpper(strings.ReplaceAll(key, "-", "_"))
		fv := v.Field(i)
		if f.Type.Kind() == reflect.Struct {
			if err := applyEnv(fv, name, lookup); err != nil {
				return err
			}
			continue
		}
		raw, ok := lookup(name)
		if !ok {
			continue
		}
		if err := setScalar(fv, raw); err != nil {
			return fmt.Errorf("environment %s: %w", name, err)
		}
	}
	return nil
}

func setScalar(fv reflect.Value, raw string) error {
	switch {
	case fv.Type() == durationType:
		d, err := time.ParseDuration(raw)
		if err != nil {
			return err
		}
		fv.SetInt(int64(d))
	case fv.Kind() == reflect.String:
		fv.SetString(raw)
	case fv.Kind() == reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return err
		}
		fv.SetBool(b)
	case fv.Kind() == reflect.Int || fv.Kind() == reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return err
		}
		fv.SetInt(n)
	case fv.Kind() == reflect.Slice && fv.Type().Elem().Kind() == reflect.String:
		parts := strings.Split(raw, ",")
		out := reflect.MakeSlice(fv.Type(), 0, len(parts))
		for _, p := range parts {
			out = reflect.Append(out, reflect.ValueOf(strings.TrimSpace(p)))
		}
		fv.Set(out)
	default:
		// Maps and other structured values are file-only by design.
	}
	return nil
}
