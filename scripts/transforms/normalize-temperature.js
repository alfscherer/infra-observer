// Vendor access points report temperature in tenths of a degree Celsius.
// Convert to the canonical environment.temperature.celsius and reject readings
// outside a sane range (a sensor fault is not an overheating event).

export const meta = {
    version: "1.1.0",
    description: "vendor.temperature.decic (tenths of C) -> environment.temperature.celsius",
    metrics: ["vendor.temperature.decic"],
};

export function transform(observation) {
    if (typeof observation.value !== "number") {
        return null;
    }
    const celsius = observation.value / 10.0;
    const min = config.get("min_celsius") ?? -40;
    const max = config.get("max_celsius") ?? 125;
    if (celsius < min || celsius > max) {
        log.warn("temperature outside plausible range; treating as a sensor fault", {celsius: String(celsius)});
        return null;
    }
    return {
        ...observation,
        metric: "environment.temperature.celsius",
        value: celsius,
        metadata: {...observation.metadata, raw_metric: observation.metric, unit: "celsius"},
    };
}
