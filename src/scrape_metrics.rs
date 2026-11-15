use crate::metrics::instruments::Registry;
use std::{fmt, sync::OnceLock, time::Instant};
pub(crate) fn registry() -> &'static Registry {
    static METRICS: OnceLock<Registry> = OnceLock::new();
    METRICS.get_or_init(Registry::default)
}
#[derive(Clone, Copy, Debug)]
pub(crate) enum Outcome {
    Success,
    HttpError,
    Timeout,
    TransportError,
    ParseError,
    ResourceLimit,
    PolicyDenied,
    ConfigError,
    Cancelled,
}
impl Outcome {
    pub fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::HttpError => "http_error",
            Self::Timeout => "timeout",
            Self::TransportError => "transport_error",
            Self::ParseError => "parse_error",
            Self::ResourceLimit => "resource_limit",
            Self::PolicyDenied => "policy_denied",
            Self::ConfigError => "config_error",
            Self::Cancelled => "cancelled",
        }
    }
}
#[derive(Debug)]
pub(crate) struct Failure(pub Outcome);
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.label())
    }
}
impl std::error::Error for Failure {}
#[derive(Debug)]
pub(crate) struct ResourceLimit(pub &'static str);
impl fmt::Display for ResourceLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ResourceLimit {}
pub(crate) struct Attempt {
    pair: &'static str,
    source: &'static str,
    outcome: Outcome,
    started: Instant,
}
impl Attempt {
    pub fn new(pair: &str, source: &str) -> Self {
        let pair = match pair {
            "service-monitor" => "service-monitor",
            "pod-monitor" => "pod-monitor",
            "scrape-config" => "scrape-config",
            "probe" => "probe",
            "internal" => "internal",
            _ => "unknown",
        };
        let source = match source {
            "victoria" => "victoria",
            "prometheus" => "prometheus",
            "internal" => "internal",
            _ => "unknown",
        };
        Self {
            pair,
            source,
            outcome: Outcome::Cancelled,
            started: Instant::now(),
        }
    }
    pub fn finish(&mut self, result: &anyhow::Result<Vec<crate::remote_write::CollectedMetric>>) {
        self.outcome = match result {
            Ok(_) => Outcome::Success,
            Err(e) => e
                .downcast_ref::<Failure>()
                .map_or(Outcome::ParseError, |f| f.0),
        };
    }
}
impl Drop for Attempt {
    fn drop(&mut self) {
        let labels = [("resource_pair", self.pair), ("source", self.source)];
        let mut outcome_labels = labels.to_vec();
        outcome_labels.push(("outcome", self.outcome.label()));
        registry().counter("metrics_agent_scrape_requests_total", &outcome_labels, 1);
        registry().observe(
            "metrics_agent_scrape_request_duration_seconds",
            &labels,
            self.started.elapsed().as_secs_f64(),
        );
    }
}
pub(crate) struct Cycle {
    pub outcome: &'static str,
    started: Instant,
}
impl Default for Cycle {
    fn default() -> Self {
        Self {
            outcome: "cancelled",
            started: Instant::now(),
        }
    }
}
impl Drop for Cycle {
    fn drop(&mut self) {
        registry().counter(
            "metrics_agent_scrape_cycles_total",
            &[("outcome", self.outcome)],
            1,
        );
        registry().observe(
            "metrics_agent_scrape_cycle_duration_seconds",
            &[],
            self.started.elapsed().as_secs_f64(),
        );
    }
}
