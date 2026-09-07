// FAULT INJECTION: a transform that throws on every call.
// Expected: each call fails and is counted; after quarantine_after consecutive
// failures the script is quarantined; observations continue unchanged; the
// service stays ready but /health/dependencies reports scripting as degraded.
export const meta = {version: "0.0.1", description: "fault injection: throws", metrics: ["system.uptime"]};
export function transform(observation) {
    throw new Error("simulated bug in an operator's script");
}
