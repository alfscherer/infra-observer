// FAULT INJECTION: a transform that never returns.
// Expected: the execution deadline interrupts it (javascript_timeouts_total
// rises), the interpreter is rebuilt, the message is processed unchanged, and
// the script is quarantined after repeated timeouts. No worker is ever stuck.
export const meta = {version: "0.0.1", description: "fault injection: infinite loop", metrics: ["system.uptime"]};
export function transform(observation) {
    for (;;) {}
}
