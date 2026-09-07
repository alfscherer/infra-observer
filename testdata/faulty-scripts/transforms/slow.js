// FAULT INJECTION: a transform that is slow but finishes just inside the
// deadline (~150ms of the default 250ms). Use it to watch script latency show up
// in javascript_execution_duration_seconds and, with a busy pipeline, executor
// saturation (javascript_saturations_total) and backpressure.
export const meta = {version: "0.0.1", description: "fault injection: slow", metrics: ["system.uptime"]};
export function transform(observation) {
    const end = Date.now() + 150;
    while (Date.now() < end) {}
    return observation;
}
