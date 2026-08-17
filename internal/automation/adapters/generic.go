package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/alfscherer/infra-observer/internal/automation"
	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/secrets"
)

// EventRecorder stores an event produced by automation.
type EventRecorder func(ctx context.Context, ev domain.Event) error

// ExtensionInvoker runs an approved integration script for a device and
// returns its message. The engine has already checked the script is on the
// approved list.
type ExtensionInvoker func(ctx context.Context, script string, dev domain.Device, a automation.Action) (string, error)

// Generic handles actions that are not specific to a device class.
type Generic struct {
	Endpoints map[string]config.Endpoint
	Secrets   secrets.Resolver
	HTTP      *http.Client
	Record    EventRecorder
	Invoke    ExtensionInvoker
	Now       func() time.Time
}

func (g *Generic) Capabilities(context.Context, domain.Device) ([]automation.Capability, error) {
	return caps("webhook", "event-record", "extension", "noop"), nil
}

func (g *Generic) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now()
}

func (g *Generic) Execute(ctx context.Context, dev domain.Device, a automation.Action) (automation.ActionResult, error) {
	switch a.Type {
	case "noop":
		return automation.ActionResult{Message: fmt.Sprintf("noop on %s (dry_run=%v)", dev.ID, a.DryRun)}, nil
	case "create_event_record":
		sev := domain.Severity(a.Params["severity"])
		if sev == "" {
			sev = domain.SeverityInfo
		}
		if a.DryRun {
			return automation.ActionResult{Message: "dry-run: would record event: " + a.Params["message"]}, nil
		}
		if g.Record == nil {
			return automation.ActionResult{}, domain.Errorf(domain.CategoryUnsupported, "no event recorder configured")
		}
		ev := domain.Event{
			EventID: domain.StableID("evt", "automation", a.RequestID), CorrelationID: a.CorrelationID, DeviceID: dev.ID,
			Type: "automation.recorded", Severity: sev, Message: a.Params["message"], OccurredAt: g.now().UTC(), Source: "automation",
		}
		if err := g.Record(ctx, ev); err != nil {
			return automation.ActionResult{}, err
		}
		return automation.ActionResult{Message: "recorded event " + ev.EventID, Details: detail("event_id", ev.EventID), Changed: true}, nil
	case "invoke_extension":
		if a.DryRun {
			return automation.ActionResult{Message: fmt.Sprintf("dry-run: would invoke extension %s for %s", a.Params["script"], dev.ID)}, nil
		}
		if g.Invoke == nil {
			return automation.ActionResult{}, domain.Errorf(domain.CategoryUnsupported, "extensions are not available in this process")
		}
		msg, err := g.Invoke(ctx, a.Params["script"], dev, a)
		if err != nil {
			return automation.ActionResult{}, err
		}
		return automation.ActionResult{Message: msg, Details: detail("script", a.Params["script"]), Changed: true}, nil
	case "send_webhook":
		return g.webhook(ctx, dev, a)
	}
	return automation.ActionResult{}, unsupported("generic", a)
}

// webhook posts a small JSON notification to a named endpoint. As with script
// HTTP, the URL and token stay in the secret store.
func (g *Generic) webhook(ctx context.Context, dev domain.Device, a automation.Action) (automation.ActionResult, error) {
	name := a.Params["endpoint"]
	if a.DryRun {
		return automation.ActionResult{Message: fmt.Sprintf("dry-run: would notify endpoint %s about %s", name, dev.ID), Details: detail("endpoint", name)}, nil
	}
	ep, ok := g.Endpoints[name]
	if !ok || g.Secrets == nil {
		return automation.ActionResult{}, domain.Errorf(domain.CategoryUnsupported, "endpoint %q is not configured", name)
	}
	secret, err := g.Secrets.Resolve(ctx, ep.URLRef)
	if err != nil {
		return automation.ActionResult{}, err
	}
	base := secret.Get("url")
	if base == "" {
		base = secret.Get("value")
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return automation.ActionResult{}, domain.Errorf(domain.CategoryPermanent, "endpoint %q is misconfigured", name)
	}
	u.Path = u.Path + "/v1/automation"
	body, _ := json.Marshal(map[string]string{"device": dev.ID, "message": a.Params["message"], "request_id": a.RequestID, "correlation_id": a.CorrelationID})
	timeout := ep.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return automation.ActionResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", a.RequestID)
	if tok := secret.Get("token"); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	client := g.HTTP
	if client == nil {
		client = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	resp, err := client.Do(req)
	if err != nil {
		return automation.ActionResult{}, domain.Wrap(domain.CategoryTransient, "webhook", fmt.Errorf("request to endpoint %q failed", name))
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode >= 500 {
		return automation.ActionResult{}, domain.Errorf(domain.CategoryTransient, "endpoint %q answered %d", name, resp.StatusCode)
	}
	if resp.StatusCode >= 300 {
		return automation.ActionResult{}, domain.Errorf(domain.CategoryPermanent, "endpoint %q answered %d", name, resp.StatusCode)
	}
	return automation.ActionResult{Message: fmt.Sprintf("notified endpoint %s (%d)", name, resp.StatusCode), Details: detail("endpoint", name, "status", fmt.Sprint(resp.StatusCode)), Changed: true}, nil
}

// NewSet assembles the standard adapter set. reader may be nil (no SNMP reads).
func NewSet(ctl Controller, reader *Reader, generic *Generic) Set {
	return Set{
		ByType: map[domain.DeviceType]automation.DeviceAdapter{
			domain.DeviceSwitch:      &Switch{Ctl: ctl, Reader: reader},
			domain.DeviceRouter:      &Switch{Ctl: ctl, Reader: reader},
			domain.DeviceAccessPoint: &AccessPoint{Ctl: ctl, Reader: reader},
			domain.DeviceServer:      &Host{Ctl: ctl, Reader: reader},
			domain.DeviceWorkstation: &Host{Ctl: ctl, Reader: reader},
		},
		Generic: generic,
	}
}
