package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/api"
	"github.com/alfscherer/infra-observer/internal/scripting/registry"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
	"github.com/alfscherer/infra-observer/internal/scripting/scripttest"
)

func cmdScript(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: infra-observer script <test|test-all|validate|list> ...")
	}
	switch args[0] {
	case "test":
		return scriptTest(args[1:])
	case "test-all":
		return scriptTestAll(args[1:])
	case "validate", "list":
		return scriptValidate(args[0], args[1:])
	}
	return fmt.Errorf("unknown script command %q", args[0])
}

// loadInventory returns the inventory named in the config, or nil if absent
// (tests that need no device context still run).
func loadInventory(cfgPath string) inventory.Reader {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil
	}
	devices, err := inventory.LoadFile(cfg.Inventory.File)
	if err != nil {
		return nil
	}
	return inventory.NewRegistry(devices)
}

func scriptTest(args []string) error {
	fs := flag.NewFlagSet("script test", flag.ContinueOnError)
	cfgPath := configFlag(fs)
	fixture := fs.String("fixture", "", "fixture JSON: a list of cases, or a single raw observation")
	timeout := fs.Duration("timeout", 250*time.Millisecond, "per-invocation execution deadline")
	// flags may follow the script path
	var positional []string
	rest := args
	for len(rest) > 0 {
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if fs.NArg() == 0 {
			break
		}
		positional = append(positional, fs.Arg(0))
		rest = fs.Args()[1:]
	}
	if len(positional) != 1 || *fixture == "" {
		return fmt.Errorf("usage: infra-observer script test <script.js> --fixture <fixture.json>")
	}
	failed, err := runFixture(positional[0], *fixture, *cfgPath, *timeout)
	if err != nil {
		return err
	}
	if failed > 0 {
		return fmt.Errorf("%d case(s) failed", failed)
	}
	return nil
}

func runFixture(script, fixture, cfgPath string, timeout time.Duration) (int, error) {
	fx, err := scripttest.LoadFixture(fixture)
	if err != nil {
		return 0, err
	}
	results, err := scripttest.Run(context.Background(), script, fx, scripttest.Options{Timeout: timeout, Inventory: loadInventory(cfgPath)})
	if err != nil {
		return 0, err
	}
	failed := 0
	fmt.Printf("%s  (%d cases) %s\n", script, len(results), fx.Description)
	for _, r := range results {
		status := "PASS"
		if !r.Pass {
			status, failed = "FAIL", failed+1
		}
		fmt.Printf("  %s  %s  (%s)\n", status, r.Name, r.Duration.Round(time.Microsecond))
		if !r.Pass {
			fmt.Printf("      %s\n", r.Message)
		}
	}
	return failed, nil
}

// scriptTestAll runs every shipped script against its fixture. A script
// without a fixture is a failure: untested extensions do not ship.
func scriptTestAll(args []string) error {
	fs := flag.NewFlagSet("script test-all", flag.ContinueOnError)
	cfgPath := configFlag(fs)
	dir := fs.String("scripts", "scripts", "scripts directory")
	fixtures := fs.String("fixtures", "testdata/scripts", "fixtures directory (<kind>/<id>.json)")
	timeout := fs.Duration("timeout", 250*time.Millisecond, "per-invocation execution deadline")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var scripts []string
	for _, kind := range []string{registry.KindTransform, registry.KindEnricher, registry.KindAutomation, registry.KindIntegration, registry.KindCollector} {
		files, _ := filepath.Glob(filepath.Join(*dir, kind, "*.js"))
		scripts = append(scripts, files...)
	}
	sort.Strings(scripts)
	if len(scripts) == 0 {
		return fmt.Errorf("no scripts found under %s", *dir)
	}
	totalFailed, missing, skipped := 0, 0, 0
	for _, s := range scripts {
		kind, id, err := scripttest.KindAndID(s)
		if err != nil {
			return err
		}
		fixture := filepath.Join(*fixtures, kind, id+".json")
		if _, err := os.Stat(fixture); err != nil {
			fmt.Printf("%s\n  FAIL  no fixture at %s\n", s, fixture)
			missing++
			continue
		}
		n, err := runFixture(s, fixture, *cfgPath, *timeout)
		if err != nil {
			if strings.Contains(err.Error(), "currently support") {
				fmt.Printf("%s\n  SKIP  %v\n", s, err)
				skipped++
				continue
			}
			return fmt.Errorf("%s: %w", s, err)
		}
		totalFailed += n
	}
	fmt.Printf("\n%d scripts, %d failed cases, %d without fixtures, %d skipped\n", len(scripts), totalFailed, missing, skipped)
	if totalFailed > 0 || missing > 0 {
		return fmt.Errorf("script tests failed")
	}
	return nil
}

// scriptValidate loads every script exactly as a service would and reports its
// status. It is the deployment gate for extensions: `validate` exits non-zero
// if any script that is supposed to be active is not.
func scriptValidate(mode string, args []string) error {
	fs := flag.NewFlagSet("script "+mode, flag.ContinueOnError)
	cfgPath := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	svc, rep := scripting.New(cfg.Scripting, cfg.Scripts, []runtime.Installer{api.Installer(api.Deps{})}, newLogger(config.Logging{Level: "error", Format: "text"}, "script"))
	defer svc.Close()
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "SCRIPT\tSTATUS\tVERSION\tMETRICS\tNOTE")
	for _, s := range svc.Registry.List() {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", s.Key, s.Status, s.Version, strings.Join(s.Metrics, ","), s.Reason)
	}
	_ = tw.Flush()
	fmt.Printf("\n%d loaded, %d disabled, %d failed\n", rep.Loaded, rep.Disabled, rep.Failed)
	if mode == "validate" && rep.Failed > 0 {
		return fmt.Errorf("%d script(s) failed validation", rep.Failed)
	}
	return nil
}
