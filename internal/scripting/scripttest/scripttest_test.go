package scripttest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alfscherer/infra-observer/internal/inventory"
)

func writeScript(t *testing.T, kind, name, src string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), kind, name+".js")
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	if err := os.WriteFile(p, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const in = `{"observation_id":"o","correlation_id":"c","device_id":"d","source":"snmp","metric":"m.x","value":1,"observed_at":"2026-08-01T12:00:00Z"}`

func run(t *testing.T, script string, cases string) []Result {
	t.Helper()
	fx := Fixture{}
	tmp := filepath.Join(t.TempDir(), "fx.json")
	_ = os.WriteFile(tmp, []byte(`{"cases":`+cases+`}`), 0o644)
	fx, err := LoadFixture(tmp)
	if err != nil {
		t.Fatal(err)
	}
	res, err := Run(context.Background(), script, fx, Options{Timeout: 80 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

const hdr = `export const meta = {version: "1", metrics: ["m.x"]}` + "\n"

func TestRunnerVerifiesExpectations(t *testing.T) {
	s := writeScript(t, "transforms", "double", hdr+"export function transform(o) { return {...o, value: o.value * 2} }\n")
	res := run(t, s, `[
	  {"name":"exact ok","input":`+in+`,"expect":{"observation_id":"o","correlation_id":"c","device_id":"d","source":"snmp","metric":"m.x","value":2,"observed_at":"2026-08-01T12:00:00Z"}},
	  {"name":"exact wrong","input":`+in+`,"expect":{"observation_id":"o","correlation_id":"c","device_id":"d","source":"snmp","metric":"m.x","value":3,"observed_at":"2026-08-01T12:00:00Z"}},
	  {"name":"subset ok","input":`+in+`,"expect_subset":{"value":2}},
	  {"name":"subset wrong","input":`+in+`,"expect_subset":{"value":5}},
	  {"name":"not unchanged","input":`+in+`,"expect":"unchanged"},
	  {"name":"not a drop","input":`+in+`,"expect":null}
	]`)
	want := []bool{true, false, true, false, false, false}
	for i, r := range res {
		if r.Pass != want[i] {
			t.Errorf("%s: pass=%v want %v (%s)", r.Name, r.Pass, want[i], r.Message)
		}
	}
}

func TestRunnerDetectsDropUnchangedAndErrors(t *testing.T) {
	drop := writeScript(t, "transforms", "drop", hdr+"export function transform(o) { return null }\n")
	if r := run(t, drop, `[{"name":"drops","input":`+in+`,"expect":null}]`)[0]; !r.Pass {
		t.Fatal(r.Message)
	}
	same := writeScript(t, "transforms", "same", hdr+"export function transform(o) { return o }\n")
	if r := run(t, same, `[{"name":"unchanged","input":`+in+`,"expect":"unchanged"}]`)[0]; !r.Pass {
		t.Fatal(r.Message)
	}
	boom := writeScript(t, "transforms", "boom", hdr+"export function transform(o) { throw new Error('kaput') }\n")
	res := run(t, boom, `[
	  {"name":"expected error","input":`+in+`,"expect_error":"kaput"},
	  {"name":"wrong fragment","input":`+in+`,"expect_error":"something else"},
	  {"name":"unexpected error","input":`+in+`,"expect":"unchanged"}
	]`)
	if !res[0].Pass || res[1].Pass || res[2].Pass || !strings.Contains(res[2].Message, "unexpected error") {
		t.Fatalf("%+v", res)
	}
}

func TestRunnerChecksTimeoutBehaviour(t *testing.T) {
	spin := writeScript(t, "transforms", "spin", hdr+"export function transform(o) { for(;;){} }\n")
	res := run(t, spin, `[{"name":"times out","input":`+in+`,"expect_timeout":true},{"name":"should not time out","input":`+in+`,"expect":"unchanged"}]`)
	if !res[0].Pass || res[1].Pass {
		t.Fatalf("%+v", res)
	}
	ok := writeScript(t, "transforms", "ok", hdr+"export function transform(o) { return o }\n")
	if r := run(t, ok, `[{"name":"claims a timeout","input":`+in+`,"expect_timeout":true}]`)[0]; r.Pass {
		t.Fatal("a script that returns normally must fail an expect_timeout case")
	}
}

func TestRunnerRejectsNondeterministicScripts(t *testing.T) {
	rnd := writeScript(t, "transforms", "rnd", hdr+"export function transform(o) { return {...o, value: Math.random()} }\n")
	r := run(t, rnd, `[{"name":"random","input":`+in+`}]`)[0]
	if r.Pass || !strings.Contains(r.Message, "deterministic") {
		t.Fatalf("%+v", r)
	}
}

func TestRunnerAppliesTheSameContractChecksAsProduction(t *testing.T) {
	evil := writeScript(t, "transforms", "evil", hdr+`export function transform(o) { return {...o, device_id: "elsewhere"} }`+"\n")
	r := run(t, evil, `[{"name":"reroutes","input":`+in+`}]`)[0]
	if r.Pass || !strings.Contains(r.Message, "device_id") {
		t.Fatalf("output shape violations must fail the test: %+v", r)
	}
}

func TestPerCaseConfigOverridesFixtureConfig(t *testing.T) {
	s := writeScript(t, "transforms", "cfg", hdr+"export function transform(o) { return {...o, value: config.get('factor')} }\n")
	tmp := filepath.Join(t.TempDir(), "fx.json")
	_ = os.WriteFile(tmp, []byte(`{"config":{"factor":2},"cases":[
	  {"name":"default","input":`+in+`,"expect_subset":{"value":2}},
	  {"name":"override","config":{"factor":7},"input":`+in+`,"expect_subset":{"value":7}}]}`), 0o644)
	fx, _ := LoadFixture(tmp)
	res, err := Run(context.Background(), s, fx, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if !r.Pass {
			t.Fatalf("%s: %s", r.Name, r.Message)
		}
	}
}

func TestRawObservationFixtureIsRunAndValidated(t *testing.T) {
	s := writeScript(t, "transforms", "id", hdr+"export function transform(o) { return o }\n")
	tmp := filepath.Join(t.TempDir(), "raw.json")
	_ = os.WriteFile(tmp, []byte(in), 0o644)
	fx, err := LoadFixture(tmp)
	if err != nil || len(fx.Cases) != 1 {
		t.Fatalf("%+v %v", fx, err)
	}
	res, err := Run(context.Background(), s, fx, Options{})
	if err != nil || !res[0].Pass || res[0].Output == "" {
		t.Fatalf("%+v %v", res, err)
	}
}

func TestBrokenScriptFailsTheWholeRun(t *testing.T) {
	bad := writeScript(t, "transforms", "bad", "export function transform( {")
	if _, err := Run(context.Background(), bad, Fixture{}, Options{}); err == nil {
		t.Fatal("a script that does not load cannot be tested")
	}
	if _, _, err := KindAndID("/tmp/whatever/x.js"); err == nil {
		t.Fatal("scripts must live in a kind directory")
	}
}

// Every script shipped in scripts/ must have a fixture and pass it. This is
// what `make test-scripts` runs from the command line; running it under
// `go test` keeps the two from drifting.
func TestShippedScriptsPassTheirFixtures(t *testing.T) {
	devs, err := inventory.LoadFile("../../../configs/inventory.yaml")
	if err != nil {
		t.Fatal(err)
	}
	reg := inventory.NewRegistry(devs)
	for _, kind := range []string{"transforms", "enrichers", "automation", "integrations"} {
		files, _ := filepath.Glob(filepath.Join("../../../scripts", kind, "*.js"))
		if len(files) == 0 {
			t.Fatalf("no %s shipped", kind)
		}
		for _, f := range files {
			id := strings.TrimSuffix(filepath.Base(f), ".js")
			fxPath := filepath.Join("../../../testdata/scripts", kind, id+".json")
			fx, err := LoadFixture(fxPath)
			if err != nil {
				t.Errorf("%s has no usable fixture: %v", f, err)
				continue
			}
			res, err := Run(context.Background(), f, fx, Options{Inventory: reg})
			if err != nil {
				t.Errorf("%s: %v", f, err)
				continue
			}
			for _, r := range res {
				if !r.Pass {
					t.Errorf("%s / %s: %s", f, r.Name, r.Message)
				}
			}
		}
	}
}
