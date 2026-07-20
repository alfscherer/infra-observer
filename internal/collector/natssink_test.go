package collector

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/schema"
	"github.com/alfscherer/infra-observer/internal/testutil"
)

func TestNATSSinkPublishesValidatedSubjectsAndDeduplicates(t *testing.T) {
	n := testutil.StartNATS(t)
	c, err := messaging.Connect(n.URL(), "test", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.EnsureStreams(ctx); err != nil {
		t.Fatal(err)
	}
	sink := NATSSink{Client: c}
	obs := []domain.Observation{
		{ObservationID: "a", CorrelationID: "c", DeviceID: "sw1", Source: "snmp", Metric: "snmp.sysName", Value: "x", ObservedAt: time.Now()},
		{ObservationID: "b", CorrelationID: "c", DeviceID: "sw1", Source: "script:vendor-http", Metric: "vendor.x", Value: 1.0, ObservedAt: time.Now()},
	}
	if err := sink.Observations(ctx, obs); err != nil {
		t.Fatal(err)
	}
	if err := sink.Observations(ctx, obs); err != nil { // retry of the same cycle
		t.Fatal(err)
	}
	if err := sink.Inventory(ctx, InventoryFact{DeviceID: "sw1", CorrelationID: "c", SysName: "sw1", ObservedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}

	stream, _ := c.JetStream().Stream(ctx, messaging.StreamTelemetryRaw)
	info, _ := stream.Info(ctx, jetstream.WithSubjectFilter(">"))
	if info.State.Msgs != 2 {
		t.Fatalf("2 unique observations expected after a retried publish, got %d", info.State.Msgs)
	}
	if info.State.Subjects["telemetry.raw.snmp"] != 1 || info.State.Subjects["telemetry.raw.script"] != 1 {
		t.Fatalf("subjects: %v", info.State.Subjects)
	}
	msg, err := stream.GetLastMsgForSubject(ctx, "telemetry.raw.snmp")
	if err != nil {
		t.Fatal(err)
	}
	if msg.Header.Get(messaging.HeaderSchema) != schema.ObservationV1 || msg.Header.Get(messaging.HeaderCorrelationID) != "c" {
		t.Fatalf("headers: %v", msg.Header)
	}
	if _, err := schema.NewValidator().DecodeObservation(msg.Data); err != nil {
		t.Fatalf("published payload must pass the consumer's validation: %v", err)
	}
	invStream, _ := c.JetStream().Stream(ctx, messaging.StreamInventory)
	invInfo, _ := invStream.Info(ctx)
	if invInfo.State.Msgs != 1 {
		t.Fatalf("inventory msgs: %d", invInfo.State.Msgs)
	}
}
