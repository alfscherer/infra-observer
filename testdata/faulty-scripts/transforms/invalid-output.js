// FAULT INJECTION: a transform that returns something that is not a valid
// observation, and one that tries to redirect data to another device.
// Expected: the result is rejected by contract validation, counted as a script
// failure (kind "output"), and the original observation continues.
export const meta = {version: "0.0.1", description: "fault injection: invalid output", metrics: ["system.uptime"]};
export function transform(observation) {
    if (observation.device_id === "server-01") {
        return {...observation, device_id: "somebody-else"};   // identity fields are immutable
    }
    return {metric: 42, value: {not: "a scalar"}};
}
