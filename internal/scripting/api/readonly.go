package api

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/dop251/goja"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
)

func stringsReader(s string) *strings.Reader { return strings.NewReader(s) }

// slogGroup turns key/value pairs into a slog group value.
func slogGroup(kv []any) slog.Value {
	attrs := make([]slog.Attr, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		attrs = append(attrs, slog.String(fmt.Sprint(kv[i]), fmt.Sprint(kv[i+1])))
	}
	return slog.GroupValue(attrs...)
}

// DeviceView is what a script may learn about a device. It deliberately omits
// credentials_ref: scripts never need to know how a device is authenticated.
type DeviceView struct {
	ID                string            `json:"id"`
	Hostname          string            `json:"hostname"`
	ManagementAddress string            `json:"management_address"`
	DeviceType        string            `json:"device_type"`
	Vendor            string            `json:"vendor"`
	Model             string            `json:"model"`
	SerialNumber      string            `json:"serial_number"`
	Site              string            `json:"site"`
	Tags              []string          `json:"tags"`
	Capabilities      []string          `json:"capabilities"`
	Enabled           bool              `json:"enabled"`
	LastSeen          string            `json:"last_seen,omitempty"`
	Attributes        map[string]string `json:"attributes"`
}

// ViewOf converts a device to its script-visible form.
func ViewOf(d domain.Device) DeviceView {
	v := DeviceView{
		ID: d.ID, Hostname: d.Hostname, ManagementAddress: d.ManagementAddress, DeviceType: string(d.DeviceType),
		Vendor: d.Vendor, Model: d.Model, SerialNumber: d.SerialNumber, Site: d.Site, Tags: d.Tags,
		Capabilities: d.Capabilities, Enabled: d.Enabled, Attributes: d.Attributes,
	}
	if v.Tags == nil {
		v.Tags = []string{}
	}
	if v.Capabilities == nil {
		v.Capabilities = []string{}
	}
	if v.Attributes == nil {
		v.Attributes = map[string]string{}
	}
	if !d.LastSeen.IsZero() {
		v.LastSeen = d.LastSeen.UTC().Format(time.RFC3339)
	}
	return v
}

func (h *host) installLog() {
	emit := func(level string) func(goja.FunctionCall) goja.Value {
		return func(fc goja.FunctionCall) goja.Value {
			c := h.call()
			if c.logLines >= MaxLogLines {
				return goja.Undefined() // over budget: dropped silently, never an error
			}
			c.logLines++
			msg := fc.Argument(0).String()
			if len(msg) > MaxLogMessage {
				msg = msg[:MaxLogMessage]
			}
			var attrs []any
			if f := fc.Argument(1); !goja.IsUndefined(f) && !goja.IsNull(f) {
				var fields map[string]any
				if err := h.fromJS(f, &fields); err == nil {
					n := 0
					for k, v := range fields {
						if n++; n > MaxLogFields {
							break
						}
						s := fmt.Sprint(v)
						if len(s) > MaxLogFieldValue {
							s = s[:MaxLogFieldValue]
						}
						attrs = append(attrs, k, s)
					}
				}
			}
			// Script-supplied fields live under their own group so a script cannot
			// forge the platform's fields (component, device_id, script_id, ...).
			l := h.d.Log.With("component", "script", "script_id", c.Script.ID, "script_version", c.Script.Version, "script_kind", c.Script.Kind)
			if c.DeviceID != "" {
				l = l.With("device_id", c.DeviceID)
			}
			if len(attrs) > 0 {
				l = l.With("fields", slogGroup(attrs))
			}
			switch level {
			case "debug":
				l.Debug(msg)
			case "warn":
				l.Warn(msg)
			case "error":
				l.Error(msg)
			default:
				l.Info(msg)
			}
			return goja.Undefined()
		}
	}
	h.object("log", map[string]func(goja.FunctionCall) goja.Value{
		"debug": emit("debug"), "info": emit("info"), "warn": emit("warn"), "error": emit("error"),
	})
}

func (h *host) installDevice() {
	h.object("device", map[string]func(goja.FunctionCall) goja.Value{
		"get": func(fc goja.FunctionCall) goja.Value {
			h.call()
			if h.d.Inventory == nil {
				return goja.Null()
			}
			d, ok := h.d.Inventory.Get(fc.Argument(0).String())
			if !ok {
				return goja.Null()
			}
			return h.toJS(ViewOf(d))
		},
	})
}

func (h *host) installInventory() {
	h.object("inventory", map[string]func(goja.FunctionCall) goja.Value{
		"lookup": func(fc goja.FunctionCall) goja.Value {
			h.call()
			if h.d.Inventory == nil {
				return h.toJS([]DeviceView{})
			}
			var q struct {
				Site       string `json:"site"`
				DeviceType string `json:"device_type"`
				Tag        string `json:"tag"`
				Enabled    *bool  `json:"enabled"`
			}
			if a := fc.Argument(0); !goja.IsUndefined(a) && !goja.IsNull(a) {
				if err := h.fromJS(a, &q); err != nil {
					h.throw("inventory.lookup: %v", err)
				}
			}
			found := h.d.Inventory.Lookup(inventory.Query{Site: q.Site, DeviceType: domain.DeviceType(q.DeviceType), Tag: q.Tag, Enabled: q.Enabled})
			out := make([]DeviceView, 0, min(len(found), MaxLookupResults))
			for _, d := range found {
				if len(out) == MaxLookupResults {
					break
				}
				out = append(out, ViewOf(d))
			}
			return h.toJS(out)
		},
	})
}

func (h *host) installState() {
	h.object("state", map[string]func(goja.FunctionCall) goja.Value{
		"get": func(fc goja.FunctionCall) goja.Value {
			h.call()
			if h.d.State == nil {
				return goja.Null()
			}
			var labels map[string]string
			if a := fc.Argument(2); !goja.IsUndefined(a) && !goja.IsNull(a) {
				if err := h.fromJS(a, &labels); err != nil {
					h.throw("state.get: labels must be an object of strings")
				}
				if labels == nil {
					labels = map[string]string{}
				}
			}
			o, err := h.d.State.Latest(h.ctx(), fc.Argument(0).String(), fc.Argument(1).String(), labels)
			if err != nil {
				h.throw("state.get: unavailable")
			}
			if o == nil {
				return goja.Null()
			}
			return h.toJS(map[string]any{"value": o.Value, "labels": o.Labels, "observed_at": o.ObservedAt.UTC().Format(time.RFC3339)})
		},
	})
}

// installConfig exposes only the calling script's own settings. Config never
// contains secrets by convention (secrets are referenced by name and resolved
// by Go), and the script cannot read another script's config.
func (h *host) installConfig() {
	h.object("config", map[string]func(goja.FunctionCall) goja.Value{
		"get": func(fc goja.FunctionCall) goja.Value {
			c := h.call()
			v, ok := c.Script.Config[fc.Argument(0).String()]
			if !ok {
				return goja.Null()
			}
			return h.toJS(v)
		},
	})
}
