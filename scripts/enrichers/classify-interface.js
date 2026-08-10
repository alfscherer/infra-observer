// Adds an `interface_role` to interface observations so rules, dashboards and
// automation policies can talk about roles ("uplink", "radio") rather than
// vendor port names.
//
// It runs after the built-in enrichment, so it can use the operator-supplied
// interface description (metadata.interface_description) as well as the name.

export const meta = {
    version: "1.0.0",
    description: "Classifies network interfaces into roles (uplink, ethernet, radio, wireless, loopback, virtual)",
    metrics: ["network.interface.*"],
};

const NAME_ROLES = [
    [/^(lo|loopback)/i, "loopback"],
    [/^(vl|vlan|irb|bvi)/i, "virtual"],
    [/^wlan/i, "wireless"],
    [/^(gi|te|fa|eth|xe|et)/i, "ethernet"],
];

export function enrich(observation, context) {
    const name = observation.labels && observation.labels.interface;
    if (!name) {
        return observation;
    }

    let role = "unknown";
    for (const [pattern, r] of NAME_ROLES) {
        if (pattern.test(name)) {
            role = r;
            break;
        }
    }
    // Wireless interfaces on an access point are its radios.
    if (role === "wireless" && context.device.device_type === "access_point") {
        role = "radio";
    }
    // An operator's description beats a naming convention.
    const description = (observation.metadata && observation.metadata.interface_description) || "";
    if (/uplink/i.test(description)) {
        role = "uplink";
    }

    return {...observation, metadata: {...observation.metadata, interface_role: role}};
}
