package sim

import (
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
