package automation

import (
	"context"
	"log/slog"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/inventory"
	"github.com/alfscherer/infra-observer/internal/schema"
)

// Integrations runs event-driven integration scripts (webhooks and the like).
// It returns an error only for platform failures; a broken integration is
// isolated by the implementation.
type Integrations interface {
	Run(ctx context.Context, ev domain.Event, dev domain.Device) error
}

// Worker handles one alert-lifecycle message: it lets policies create
// automation requests and lets integrations react. Both are idempotent per
// event, so redelivery is safe.
type Worker struct {
	Engine       *Engine
	Integrations Integrations // optional
	Devices      inventory.Reader
	Log          *slog.Logger
}

// Handle processes one events.alert message. A malformed message is a
// validation error (poison, to be dead-lettered); platform failures are
// retryable errors.
func (w *Worker) Handle(ctx context.Context, data []byte) error {
	ev, err := schema.Decode[domain.Event](data, schema.DefaultLimits().MaxMessageBytes)
	if err != nil {
		return err
	}
	if ev.EventID == "" || ev.DeviceID == "" || ev.Type == "" {
		return domain.Errorf(domain.CategoryValidation, "event is missing event_id, device_id or type")
	}
	if _, err := w.Engine.HandleEvent(ctx, ev); err != nil {
		return err
	}
	if w.Integrations != nil {
		if dev, ok := w.Devices.Get(ev.DeviceID); ok {
			if err := w.Integrations.Run(ctx, ev, dev); err != nil {
				return err
			}
		}
	}
	return nil
}
