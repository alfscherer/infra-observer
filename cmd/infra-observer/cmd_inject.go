package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/alfscherer/infra-observer/internal/config"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/schema"
)

// cmdInject publishes hostile or awkward telemetry into a running system, to
// watch how it copes. It publishes straight to the raw subject, exactly where a
// misbehaving collector would, and uses random message ids so JetStream's
// publish-side de-duplication does not mask what the consumers do.
func cmdInject(args []string) error {
	fs := flag.NewFlagSet("inject", flag.ContinueOnError)
	path := configFlag(fs)
	kind := fs.String("kind", "", "malformed | poison | unknown-device | future | duplicate")
	device := fs.String("device", "server-01", "device id for duplicate")
	count := fs.Int("count", 1, "how many messages to publish")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch *kind {
	case "malformed", "poison", "unknown-device", "future", "duplicate":
	default:
		return fmt.Errorf("unknown --kind %q (malformed, poison, unknown-device, future, duplicate)", *kind)
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	client, err := messaging.Connect(cfg.NATS.URL, "inject", newLogger(config.Logging{Level: "error", Format: "text"}, "cli"))
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now := time.Now().UTC()
	valid := func(id, dev string, at time.Time) domain.Observation {
		return domain.Observation{ObservationID: id, CorrelationID: "inject-" + id, DeviceID: dev, Source: "snmp",
			Metric: "snmp.sysUpTime", Value: 123.0, ObservedAt: at}
	}
	publish := func(payload []byte) error {
		return client.Publish(ctx, messaging.SubjectRawSNMP, domain.NewCorrelationID(), payload, map[string]string{messaging.HeaderSchema: schema.ObservationV1})
	}
	for i := 0; i < *count; i++ {
		var payload []byte
		switch *kind {
		case "malformed":
			payload = []byte("this is not json {")
		case "poison":
			payload = []byte(`{"observation_id":"poison","metric":"Not A Metric","value":{"nested":true}}`)
		case "unknown-device":
			payload, _ = schema.Encode(valid(domain.NewCorrelationID(), "ghost-99", now))
		case "future":
			payload, _ = schema.Encode(valid(domain.NewCorrelationID(), *device, now.Add(72*time.Hour)))
		case "duplicate":
			// the same observation id every time; only the JetStream message id differs
			payload, _ = schema.Encode(valid("injected-duplicate", *device, now))
		default:
			return fmt.Errorf("unknown --kind %q (malformed, poison, unknown-device, future, duplicate)", *kind)
		}
		if err := publish(payload); err != nil {
			return err
		}
	}
	fmt.Printf("published %d %s message(s) to %s\n", *count, *kind, messaging.SubjectRawSNMP)
	switch *kind {
	case "duplicate":
		fmt.Println("expect: exactly one stored observation with id 'injected-duplicate' (see /api/devices/" + *device + "/metrics?metric=system.uptime)")
	default:
		fmt.Println("expect: the message(s) end up in the dead-letter stream, not in an endless retry loop: infra-observer deadletter list")
	}
	return nil
}
