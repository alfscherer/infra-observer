// Posts alert events to a webhook, for example a chat bridge or a ticketing
// intake. The endpoint is named in configuration (`endpoint_ref`); the script
// never sees its URL or token, and cannot call any endpoint it has not been
// granted.
//
// Delivery is at-least-once. The platform adds an Idempotency-Key header that
// is stable for (script, event), so the receiver can discard duplicates.

export const meta = {
    version: "1.0.0",
    description: "Posts warning-and-above events to the configured webhook",
    events: ["*"],
    min_severity: "warning",
};

export function handleEvent(event, context) {
    const endpoint = config.get("endpoint_ref");
    if (!endpoint) {
        return {ok: false, message: "no endpoint_ref configured"};
    }

    const response = http.request({
        endpoint: endpoint,
        method: "POST",
        path: "/v1/alerts",
        body: {
            summary: event.message,
            severity: event.severity,
            type: event.type,
            resolved: Boolean(event.resolves),
            device: event.device_id,
            hostname: context.device ? context.device.hostname : event.device_id,
            site: context.device ? context.device.site : "",
            occurred_at: event.occurred_at,
            correlation_id: event.correlation_id,
        },
    });

    if (!response.ok) {
        log.warn("webhook rejected the event", {status: String(response.status)});
        return {ok: false, message: "webhook returned " + response.status};
    }
    return {ok: true};
}
