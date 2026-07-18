package messaging

// Subject and stream names. Subjects are part of the platform contract and
// documented in PIPELINE.md; they are constants, not configuration.
const (
	SubjectRawPrefix         = "telemetry.raw" // telemetry.raw.<source>
	SubjectRawSNMP           = "telemetry.raw.snmp"
	SubjectRawScript         = "telemetry.raw.script"
	SubjectNormalized        = "telemetry.normalized"
	SubjectInventoryObserved = "inventory.observed"
	SubjectEventDevice       = "events.device"
	SubjectEventAlert        = "events.alert"
	SubjectAutomationRequest = "automation.request"
	SubjectAutomationResult  = "automation.result"
	SubjectDeadLetter        = "system.deadletter"

	// SubjectSimControl carries scenario commands to the simulator. It is core
	// NATS (not JetStream): a scenario that nobody heard is not worth replaying.
	SubjectSimControl = "sim.control"

	StreamTelemetryRaw        = "TELEMETRY_RAW"
	StreamTelemetryNormalized = "TELEMETRY_NORMALIZED"
	StreamInventory           = "INVENTORY"
	StreamEvents              = "EVENTS"
	StreamAutomation          = "AUTOMATION"
	StreamDeadLetter          = "DEADLETTER"
)

// Header names carried on messages.
const (
	HeaderMsgID         = "Nats-Msg-Id" // JetStream publish-side de-duplication key
	HeaderCorrelationID = "X-Correlation-Id"
	HeaderSchema        = "X-Schema"
)
