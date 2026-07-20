package collector

import (
	"context"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/schema"
)

// NATSSink publishes collector output to JetStream. The observation ID is the
// Nats-Msg-Id, so a publisher retry inside the duplicate window is dropped by
// the server; consumers still tolerate duplicates beyond that.
type NATSSink struct {
	Client *messaging.Client
}

func (s NATSSink) Observations(ctx context.Context, obs []domain.Observation) error {
	msgs := make([]messaging.OutMsg, 0, len(obs))
	for _, o := range obs {
		payload, err := schema.Encode(o)
		if err != nil {
			return err
		}
		msgs = append(msgs, messaging.OutMsg{
			Subject: messaging.SubjectRawPrefix + "." + rootSource(o.Source),
			MsgID:   o.ObservationID,
			Payload: payload,
			Headers: map[string]string{
				messaging.HeaderSchema:        schema.ObservationV1,
				messaging.HeaderCorrelationID: o.CorrelationID,
			},
		})
	}
	return s.Client.PublishBatch(ctx, msgs)
}

func (s NATSSink) Inventory(ctx context.Context, f InventoryFact) error {
	payload, err := schema.Encode(f)
	if err != nil {
		return err
	}
	return s.Client.Publish(ctx, messaging.SubjectInventoryObserved,
		domain.StableID("inv", f.CorrelationID, f.DeviceID), payload,
		map[string]string{messaging.HeaderSchema: schema.InventoryV1, messaging.HeaderCorrelationID: f.CorrelationID})
}

// rootSource maps "script:vendor-http" to "script" so subjects stay a small,
// known set (telemetry.raw.snmp, telemetry.raw.script).
func rootSource(source string) string {
	for i := 0; i < len(source); i++ {
		if source[i] == ':' {
			return source[:i]
		}
	}
	return source
}
