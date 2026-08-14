// Proposes a remediation for an interface that stayed down.
//
// This script only *proposes*. The Go automation engine still decides whether
// the proposal is allowed: it must be one of the policy's allowed_actions,
// pass parameter validation, and clear every gate (tag, capability, allowlist,
// maintenance window, cooldown, retry limit, dry-run). A wrong or malicious
// answer from here ends as an audited denial, never as an action.
//
// What the script contributes is judgment that is awkward to express as static
// configuration: which interfaces are safe to touch at all.

export const meta = {
    version: "1.0.0",
    description: "Proposes bouncing a down lab interface, except uplinks",
};

export function proposeAutomation(event, device, context) {
    const iface = event.labels && event.labels.interface;
    if (!iface) {
        return null;
    }

    // Bouncing the port that carries our own management traffic can cut the
    // switch off from the platform. Uplinks are never touched automatically.
    const description = (device.attributes && device.attributes["interface." + iface + ".description"]) || "";
    if (/uplink/i.test(description)) {
        log.info("not proposing remediation for an uplink", {interface: iface});
        return null;
    }

    // Lab devices only; production remediation is a human decision.
    if (device.site !== "lab" || !device.tags.includes("automation-enabled")) {
        return null;
    }

    // If the port is already administratively disabled, someone did that on
    // purpose. Do not fight them.
    const admin = state.get(device.id, "network.interface.admin_enabled", {interface: iface, if_index: event.labels.if_index});
    if (admin && admin.value === false) {
        return null;
    }

    return {
        action: "bounce_interface",
        target: iface,
        reason: "Interface " + iface + " on " + device.hostname + " remained down beyond the policy threshold",
    };
}
