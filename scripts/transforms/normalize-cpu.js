// Normalizes the CPU load a vendor's firmware reports as a percentage (0..100)
// into the platform's canonical utilization ratio (0..1).
//
// Why a script and not a Go mapping: the AP profile (configs/profiles/vendor-ap.yaml)
// reports `vendor.cpu.load`, which the declarative normalization table does not
// know about. This is the extension point for exactly that kind of quirk.

export const meta = {
    version: "1.0.0",
    description: "vendor.cpu.load (percent) -> system.cpu.utilization (ratio 0..1)",
    // Go only calls this script for these metrics.
    metrics: ["vendor.cpu.load"],
};

export function transform(observation) {
    if (typeof observation.value !== "number" || Number.isNaN(observation.value)) {
        // A reading we cannot interpret is dropped rather than guessed at.
        log.warn("dropping non-numeric cpu load", {value: String(observation.value)});
        return null;
    }
    // Firmware occasionally reports slightly above 100 under load; clamp.
    const ratio = Math.min(Math.max(observation.value / 100.0, 0), 1);
    return {
        ...observation,
        metric: "system.cpu.utilization",
        value: ratio,
        metadata: {...observation.metadata, raw_metric: observation.metric, unit: "ratio"},
    };
}
