package health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func ok(context.Context) Result   { return Result{Status: OK} }
func down(context.Context) Result { return Result{Status: Down, Detail: "connection refused"} }
func degraded(context.Context) Result {
	return Result{Status: Degraded, Detail: "1 script quarantined"}
}

func get(t *testing.T, h http.Handler) (int, Report) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	var rep Report
	_ = json.Unmarshal(rec.Body.Bytes(), &rep)
	return rec.Code, rep
}

func TestReadyWhenAllCriticalDependenciesAreUp(t *testing.T) {
	c := NewChecker("v1", Check{"postgres", true, ok}, Check{"nats", true, ok}, Check{"scripting", false, ok})
	code, rep := get(t, c.Readiness())
	if code != 200 || !rep.Ready || rep.Status != "ready" || len(rep.Checks) != 3 || rep.Version != "v1" {
		t.Fatalf("%d %+v", code, rep)
	}
}

func TestCriticalFailureMakesTheServiceUnreadyButNotDead(t *testing.T) {
	c := NewChecker("v1", Check{"postgres", true, down}, Check{"nats", true, ok})
	code, rep := get(t, c.Readiness())
	if code != 503 || rep.Ready || rep.Status != "unready" || rep.Checks["postgres"].Detail != "connection refused" {
		t.Fatalf("%d %+v", code, rep)
	}
	if code, _ := get(t, c.Liveness()); code != 200 {
		t.Fatalf("a process whose database is down is alive (restarting it would not help): %d", code)
	}
}

func TestBrokenOptionalExtensionDegradesButStaysReady(t *testing.T) {
	c := NewChecker("v1", Check{"postgres", true, ok}, Check{"scripting", false, degraded})
	code, rep := get(t, c.Readiness())
	if code != 200 || !rep.Ready || rep.Status != "degraded" || rep.Checks["scripting"].Status != Degraded {
		t.Fatalf("an optional extension must not take the service out of rotation: %d %+v", code, rep)
	}
	c = NewChecker("v1", Check{"postgres", true, ok}, Check{"scripting", false, down})
	if code, rep := get(t, c.Readiness()); code != 200 || rep.Status != "degraded" {
		t.Fatalf("even a fully down optional check only degrades: %d %+v", code, rep)
	}
}

func TestDependenciesEndpointIsAlways200WithDetail(t *testing.T) {
	c := NewChecker("v1", Check{"postgres", true, down})
	code, rep := get(t, c.Dependencies())
	if code != 200 || rep.Status != "unready" || rep.Checks["postgres"].LatencyMS < 0 || !rep.Checks["postgres"].Critical {
		t.Fatalf("%d %+v", code, rep)
	}
}

func TestHungAndPanickingChecksCannotHangOrCrashTheEndpoint(t *testing.T) {
	c := NewChecker("v1",
		Check{"hangs", true, func(ctx context.Context) Result { <-time.After(time.Minute); return Result{Status: OK} }},
		Check{"panics", false, func(context.Context) Result { panic("boom") }},
		Check{"fine", true, ok})
	c.Timeout = 100 * time.Millisecond
	start := time.Now()
	code, rep := get(t, c.Readiness())
	if time.Since(start) > 2*time.Second {
		t.Fatalf("the endpoint must answer within the check timeout, took %v", time.Since(start))
	}
	if code != 503 || rep.Checks["hangs"].Status != Down || rep.Checks["panics"].Status != Down || rep.Checks["fine"].Status != OK {
		t.Fatalf("%d %+v", code, rep)
	}
}

func TestChecksRunConcurrently(t *testing.T) {
	slow := func(context.Context) Result { time.Sleep(150 * time.Millisecond); return Result{Status: OK} }
	c := NewChecker("v1", Check{"a", true, slow}, Check{"b", true, slow}, Check{"c", true, slow}, Check{"d", true, slow})
	start := time.Now()
	c.Run(context.Background())
	if time.Since(start) > 400*time.Millisecond {
		t.Fatalf("four 150ms checks should overlap, took %v", time.Since(start))
	}
}

func TestMountRegistersStandardEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	NewChecker("v1", Check{"x", true, ok}).Mount(mux)
	for _, p := range []string{"/healthz", "/readyz", "/health/dependencies"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", p, nil))
		if rec.Code != 200 {
			t.Errorf("%s: %d", p, rec.Code)
		}
	}
}
