// Package telemetry is the platform's self-monitoring: Prometheus metrics for
// every subsystem, with deliberately low-cardinality labels (worker, script,
// policy, category). Per-device and per-observation detail belongs in logs and
// traces by ID, not in metric labels.
package telemetry

import (
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/alfscherer/infra-observer/internal/collector"
	"github.com/alfscherer/infra-observer/internal/domain"
	"github.com/alfscherer/infra-observer/internal/messaging"
	"github.com/alfscherer/infra-observer/internal/scripting"
	"github.com/alfscherer/infra-observer/internal/scripting/runtime"
)

var durationBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Metrics holds every application metric of a process.
type Metrics struct {
	Registry *prometheus.Registry

	CollectorRequests *prometheus.CounterVec // protocol, result
	CollectorFailures *prometheus.CounterVec // category
	CollectorDuration *prometheus.HistogramVec

	QueueDepth        *prometheus.GaugeVec     // consumer: unprocessed messages waiting in JetStream
	QueueAckPending   *prometheus.GaugeVec     // consumer: delivered, not yet acknowledged
	WorkerInFlight    *prometheus.GaugeVec     // worker
	WorkerBuffered    *prometheus.GaugeVec     // worker
	WorkerCapacity    *prometheus.GaugeVec     // worker
	MessagesProcessed *prometheus.CounterVec   // worker
	MessagesFailed    *prometheus.CounterVec   // worker, outcome (retry, dead_letter, dlq_failed)
	ProcessingSeconds *prometheus.HistogramVec // worker

	JSExecutions *prometheus.CounterVec   // script
	JSFailures   *prometheus.CounterVec   // script, kind
	JSTimeouts   *prometheus.CounterVec   // script
	JSDuration   *prometheus.HistogramVec // script
	JSQuarantine *prometheus.CounterVec   // script
	JSScripts    *prometheus.GaugeVec     // status
	JSSaturation prometheus.Counter

	AutomationRequests *prometheus.CounterVec // policy
	AutomationResults  *prometheus.CounterVec // policy, status
	AutomationFailures *prometheus.CounterVec // policy

	DatabaseErrors  *prometheus.CounterVec // category
	OutboxPublished prometheus.Counter
}

// New creates a Metrics with its own registry (plus Go runtime and process
// collectors). A private registry keeps tests independent and avoids global state.
func New() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := &Metrics{Registry: r}
	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		r.MustRegister(v)
		return v
	}
	gauge := func(name, help string, labels ...string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
		r.MustRegister(v)
		return v
	}
	hist := func(name, help string, labels ...string) *prometheus.HistogramVec {
		v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: durationBuckets}, labels)
		r.MustRegister(v)
		return v
	}
	m.CollectorRequests = counter("collector_requests_total", "Device poll attempts by protocol and result.", "protocol", "result")
	m.CollectorFailures = counter("collector_failures_total", "Failed device polls by error category.", "category")
	m.CollectorDuration = hist("collector_poll_duration_seconds", "Duration of device polls.", "protocol")

	m.QueueDepth = gauge("observation_queue_depth", "Messages waiting in JetStream for a consumer (the backlog: collection outpacing processing shows up here).", "consumer")
	m.QueueAckPending = gauge("observation_queue_ack_pending", "Messages delivered to a consumer and not yet acknowledged.", "consumer")
	m.WorkerInFlight = gauge("worker_in_flight", "Messages currently being handled.", "worker")
	m.WorkerBuffered = gauge("worker_buffered", "Messages fetched and waiting for a free shard.", "worker")
	m.WorkerCapacity = gauge("worker_capacity", "Most messages a worker will hold (shards x (queue + 1)).", "worker")
	m.MessagesProcessed = counter("messages_processed_total", "Messages handled successfully.", "worker")
	m.MessagesFailed = counter("messages_failed_total", "Messages that did not succeed: retried, dead-lettered, or dead-letter publish failed.", "worker", "outcome")
	m.ProcessingSeconds = hist("processing_duration_seconds", "Time spent handling one message.", "worker")

	m.JSExecutions = counter("javascript_executions_total", "Script invocations.", "script")
	m.JSFailures = counter("javascript_failures_total", "Script invocations that failed, by kind (exception, timeout, panic, output, ...).", "script", "kind")
	m.JSTimeouts = counter("javascript_timeouts_total", "Script invocations that exceeded their execution deadline.", "script")
	m.JSDuration = hist("javascript_execution_duration_seconds", "Script execution time.", "script")
	m.JSQuarantine = counter("javascript_quarantines_total", "Scripts quarantined after repeated failures.", "script")
	m.JSScripts = gauge("javascript_scripts", "Scripts by status.", "status")
	m.JSSaturation = prometheus.NewCounter(prometheus.CounterOpts{Name: "javascript_saturations_total", Help: "Calls rejected because the script executor was saturated."})
	r.MustRegister(m.JSSaturation)

	m.AutomationRequests = counter("automation_requests_total", "Automation requests recorded, by policy.", "policy")
	m.AutomationResults = counter("automation_results_total", "Automation outcomes by policy and status.", "policy", "status")
	m.AutomationFailures = counter("automation_failures_total", "Automation actions that failed while executing.", "policy")

	m.DatabaseErrors = counter("database_errors_total", "Database errors by category.", "category")
	m.OutboxPublished = prometheus.NewCounter(prometheus.CounterOpts{Name: "outbox_published_total", Help: "Outbox messages published to NATS."})
	r.MustRegister(m.OutboxPublished)
	return m
}

// Handler serves the Prometheus text exposition.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}

// OnPoll is the collector.Scheduler hook.
func (m *Metrics) OnPoll(e collector.PollEvent) {
	result := "success"
	if e.Err != nil {
		result = "failure"
		m.CollectorFailures.WithLabelValues(string(domain.CategoryOf(e.Err))).Inc()
	}
	m.CollectorRequests.WithLabelValues("snmp", result).Inc()
	m.CollectorDuration.WithLabelValues("snmp").Observe(e.Duration.Seconds())
}

// WorkerOutcome is the messaging.WorkerOptions.OnOutcome hook for a named worker.
func (m *Metrics) WorkerOutcome(worker string) func(outcome, subject string, d time.Duration) {
	return func(outcome, _ string, d time.Duration) {
		m.ProcessingSeconds.WithLabelValues(worker).Observe(d.Seconds())
		if outcome == "processed" {
			m.MessagesProcessed.WithLabelValues(worker).Inc()
			return
		}
		m.MessagesFailed.WithLabelValues(worker, outcome).Inc()
	}
}

// AttachWorker exposes a worker's live gauges, sampled at scrape time.
func (m *Metrics) AttachWorker(name string, w *messaging.Worker) {
	m.Registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "worker_in_flight_now", Help: "Messages being handled right now.", ConstLabels: prometheus.Labels{"worker": name}},
			func() float64 { return float64(w.Stats().InFlight) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "worker_buffered_now", Help: "Messages waiting for a free shard right now.", ConstLabels: prometheus.Labels{"worker": name}},
			func() float64 { return float64(w.Stats().QueueDepth) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "worker_capacity_now", Help: "Most messages the worker will hold.", ConstLabels: prometheus.Labels{"worker": name}},
			func() float64 { return float64(w.Stats().Capacity) }),
	)
}

// ScriptResult is the scripting.Service.OnResult hook.
func (m *Metrics) ScriptResult(key string, d time.Duration, err error) {
	m.JSExecutions.WithLabelValues(key).Inc()
	if d > 0 {
		m.JSDuration.WithLabelValues(key).Observe(d.Seconds())
	}
	if err == nil {
		return
	}
	if errors.Is(err, runtime.ErrSaturated) {
		m.JSSaturation.Inc()
		return
	}
	var se *runtime.Error
	if errors.As(err, &se) {
		m.JSFailures.WithLabelValues(key, se.Kind).Inc()
		if se.Kind == runtime.KindTimeout {
			m.JSTimeouts.WithLabelValues(key).Inc()
		}
		return
	}
	if domain.CategoryOf(err) == domain.CategoryScript {
		m.JSFailures.WithLabelValues(key, "other").Inc()
	}
}

// AttachScripting wires a scripting service into the metrics.
func (m *Metrics) AttachScripting(svc *scripting.Service) {
	svc.OnResult = m.ScriptResult
	svc.OnQuarantine = func(key string) { m.JSQuarantine.WithLabelValues(key).Inc() }
	m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "javascript_executor_queue_depth", Help: "Script invocations waiting for an executor."},
		func() float64 { return float64(svc.Pool.Stats().QueueDepth) }))
	for _, s := range []string{"active", "disabled", "quarantined", "failed"} {
		s := s
		m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "javascript_scripts_now", Help: "Scripts by status.", ConstLabels: prometheus.Labels{"status": s}},
			func() float64 {
				h := svc.Health()
				switch s {
				case "active":
					return float64(h.Loaded)
				case "disabled":
					return float64(h.Disabled)
				case "quarantined":
					return float64(h.Quarantined)
				}
				return float64(h.Failed)
			}))
	}
}

// AutomationRequest and AutomationResult are the automation.Engine hooks.
func (m *Metrics) AutomationRequest(policy string) {
	m.AutomationRequests.WithLabelValues(policy).Inc()
}

func (m *Metrics) AutomationResult(policy string, status domain.AutomationStatus, _ error) {
	m.AutomationResults.WithLabelValues(policy, string(status)).Inc()
	if status == domain.AutomationFailed {
		m.AutomationFailures.WithLabelValues(policy).Inc()
	}
}

// DatabaseError is the persistence.PGStore.OnError hook.
func (m *Metrics) DatabaseError(err error) {
	m.DatabaseErrors.WithLabelValues(string(domain.CategoryOf(err))).Inc()
}
