use crate::metrics::instruments::Registry;
use std::sync::OnceLock;
pub(crate) fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(Registry::default)
}
pub(crate) fn request(pair: &'static str, source: &'static str, outcome: &'static str) {
    registry().counter(
        "metrics_agent_discovery_requests_total",
        &[
            ("resource_pair", pair),
            ("source", source),
            ("outcome", outcome),
        ],
        1,
    );
}
pub(crate) fn refresh(outcome: &'static str) {
    registry().counter(
        "metrics_agent_discovery_refreshes_total",
        &[("outcome", outcome)],
        1,
    );
}
pub(crate) fn cache(state: &'static str, age: f64) {
    registry().counter(
        "metrics_agent_discovery_cache_uses_total",
        &[("state", state)],
        1,
    );
    registry().gauge("metrics_agent_discovery_cache_age_seconds", &[], age);
}
