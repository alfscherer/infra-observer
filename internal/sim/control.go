package sim

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/messaging"
)

// controlReply answers a scenario request.
type controlReply struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}

// ServeControl subscribes to scenario commands on sim.control (core NATS,
// request/reply). Scenarios are operator commands, not telemetry: nobody
// benefits from replaying one, so they deliberately bypass JetStream.
func (w *World) ServeControl(nc *nats.Conn) (*nats.Subscription, error) {
	return nc.Subscribe(messaging.SubjectSimControl, func(m *nats.Msg) {
		reply := controlReply{OK: true}
		var s Scenario
		if err := json.Unmarshal(m.Data, &s); err != nil {
			reply = controlReply{Message: "malformed scenario: " + err.Error()}
		} else if msg, err := w.Apply(s); err != nil {
			reply = controlReply{Message: err.Error()}
		} else {
			reply.Message = msg
		}
		if m.Reply != "" {
			b, _ := json.Marshal(reply)
			_ = m.Respond(b)
		}
	})
}

// Controller applies scenarios to a simulator, whether in-process or over NATS.
type Controller interface {
	Apply(ctx context.Context, s Scenario) (string, error)
}

// NATSController drives a running simulator through its control subject. The
// mock device adapters use it so that an automation action visibly changes
// the simulated device, closing the loop from alert to recovery.
type NATSController struct {
	Conn    *nats.Conn
	Timeout time.Duration
}

func (c NATSController) Apply(_ context.Context, s Scenario) (string, error) {
	t := c.Timeout
	if t <= 0 {
		t = 3 * time.Second
	}
	return SendScenario(c.Conn, s, t)
}

// SendScenario asks a running simulator to apply s.
func SendScenario(nc *nats.Conn, s Scenario, timeout time.Duration) (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	msg, err := nc.Request(messaging.SubjectSimControl, b, timeout)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrTimeout) {
			return "", domain.Errorf(domain.CategoryDependency, "no simulator answered on %s (is it running?)", messaging.SubjectSimControl)
		}
		return "", err
	}
	var r controlReply
	if err := json.Unmarshal(msg.Data, &r); err != nil {
		return "", err
	}
	if !r.OK {
		return "", errors.New(r.Message)
	}
	return r.Message, nil
}

// DeviceController adapts a Controller to the operation shape the automation
// adapters use: Do(ctx, device, event, args).
type DeviceController struct{ C Controller }

func (d DeviceController) Do(ctx context.Context, device, event string, args map[string]string) error {
	_, err := d.C.Apply(ctx, Scenario{Device: device, Event: event, Args: args})
	return err
}
