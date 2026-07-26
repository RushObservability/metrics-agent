use crate::status::StatusSnapshot;

pub fn render(snapshot: &StatusSnapshot) -> String {
    let mut output = String::with_capacity(4096);
    gauge(
        &mut output,
        "metrics_agent_ready",
        "Whether informer caches are synchronized.",
        snapshot.ready as u64,
    );
    gauge(
        &mut output,
        "metrics_agent_uptime_seconds",
        "Process uptime in seconds.",
        snapshot.uptime_seconds,
    );
    counter(
        &mut output,
        "metrics_agent_precedence_reconciliations_total",
        "Total scrape precedence reconciliations.",
        snapshot.counters.reconciliations,
    );
    counter(
        &mut output,
        "metrics_agent_precedence_patches_victoriametrics_total",
        "Total precedence patches selecting native VictoriaMetrics configuration.",
        snapshot.counters.patches_victoria,
    );
    counter(
        &mut output,
        "metrics_agent_precedence_patches_prometheus_total",
        "Total precedence patches restoring Prometheus conversion.",
        snapshot.counters.patches_prometheus,
    );
    counter(
        &mut output,
        "metrics_agent_precedence_errors_total",
        "Total precedence reconciliation and collection errors.",
        snapshot.counters.errors,
    );
    counter(
        &mut output,
        "metrics_agent_reconciliations_total",
        "Total reconciliation attempts.",
        snapshot.counters.reconciliations,
    );
    output.push_str("# HELP metrics_agent_patches_total Total successful precedence patches by source.\n# TYPE metrics_agent_patches_total counter\n");
    counter_line(
        &mut output,
        "metrics_agent_patches_total",
        &[("source", "victoriametrics")],
        snapshot.counters.patches_victoria,
    );
    counter_line(
        &mut output,
        "metrics_agent_patches_total",
        &[("source", "prometheus")],
        snapshot.counters.patches_prometheus,
    );
    gauge_float(
        &mut output,
        "metrics_agent_process_cpu_percent",
        "Controller process CPU utilization percentage.",
        snapshot.process.cpu_percent,
    );
    gauge(
        &mut output,
        "metrics_agent_process_resident_memory_bytes",
        "Controller process resident memory in bytes.",
        snapshot.process.memory_bytes,
    );
    gauge(
        &mut output,
        "metrics_agent_scrape_targets",
        "Number of discovered Prometheus scrape targets.",
        snapshot.scrape.targets,
    );
    gauge(
        &mut output,
        "metrics_agent_scrape_healthy_targets",
        "Number of discovered scrape targets that returned successfully.",
        snapshot.scrape.healthy_targets,
    );
    gauge(
        &mut output,
        "metrics_agent_scrape_samples",
        "Number of samples collected during the last scrape cycle.",
        snapshot.scrape.samples,
    );
    gauge(
        &mut output,
        "metrics_agent_scrape_errors",
        "Number of scrape targets that failed during the last scrape cycle.",
        snapshot.scrape.errors,
    );

    output.push_str("# HELP metrics_agent_crd_available Whether the configured scrape CRD is available.\n# TYPE metrics_agent_crd_available gauge\n");
    output.push_str("# HELP metrics_agent_observed_crd_objects Number of observed scrape CRD objects.\n# TYPE metrics_agent_observed_crd_objects gauge\n");
    output.push_str("# HELP metrics_agent_native_victoriametrics_objects Number of native VictoriaMetrics scrape objects.\n# TYPE metrics_agent_native_victoriametrics_objects gauge\n");
    output.push_str("# HELP metrics_agent_converted_victoriametrics_objects Number of converter-owned VictoriaMetrics scrape objects.\n# TYPE metrics_agent_converted_victoriametrics_objects gauge\n");

    for pair in &snapshot.resource_pairs {
        let label = escape_label(&pair.name);
        gauge_line(
            &mut output,
            "metrics_agent_crd_available",
            &[("resource_pair", &label), ("source", "prometheus")],
            pair.prometheus_available as u64,
        );
        gauge_line(
            &mut output,
            "metrics_agent_crd_available",
            &[("resource_pair", &label), ("source", "victoriametrics")],
            pair.victoria_available as u64,
        );
        gauge_line(
            &mut output,
            "metrics_agent_observed_crd_objects",
            &[("resource_pair", &label), ("source", "prometheus")],
            pair.prometheus_count,
        );
        gauge_line(
            &mut output,
            "metrics_agent_observed_crd_objects",
            &[("resource_pair", &label), ("source", "victoriametrics")],
            pair.victoria_count,
        );
        gauge_line(
            &mut output,
            "metrics_agent_native_victoriametrics_objects",
            &[("resource_pair", &label)],
            pair.native_count,
        );
        gauge_line(
            &mut output,
            "metrics_agent_converted_victoriametrics_objects",
            &[("resource_pair", &label)],
            pair.converted_count,
        );
    }
    if let Some(series) = snapshot.totals.metric_series {
        gauge(
            &mut output,
            "metrics_agent_remote_write_series",
            "Metric series included in the last Rush remote-write payload.",
            series,
        );
    }
    gauge(
        &mut output,
        "metrics_agent_remote_write_enabled",
        "Whether direct Rush remote write is configured.",
        snapshot.remote_write.enabled as u64,
    );
    gauge(
        &mut output,
        "metrics_agent_remote_write_up",
        "Whether the last direct Rush remote-write attempt succeeded.",
        (snapshot.remote_write.enabled
            && snapshot.remote_write.last_publish_at.is_some()
            && snapshot.remote_write.last_error.is_none()) as u64,
    );
    if let Some(samples) = snapshot.remote_write.last_samples {
        gauge(
            &mut output,
            "metrics_agent_remote_write_samples",
            "Samples included in the last Rush remote-write payload.",
            samples,
        );
    }
    output
}

fn counter(output: &mut String, name: &str, help: &str, value: u64) {
    output.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
    ));
}

fn gauge(output: &mut String, name: &str, help: &str, value: u64) {
    output.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n"
    ));
}

fn gauge_float(output: &mut String, name: &str, help: &str, value: f64) {
    output.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value:.3}\n"
    ));
}

fn gauge_line(output: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    let labels = labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{value}\""))
        .collect::<Vec<_>>()
        .join(",");
    output.push_str(&format!("{name}{{{labels}}} {value}\n"));
}

fn counter_line(output: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    let labels = labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{value}\""))
        .collect::<Vec<_>>()
        .join(",");
    output.push_str(&format!("{name}{{{labels}}} {value}\n"));
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::{escape_label, render};
    use crate::status::{
        CountersStatus, CrdObjectStatus, ProcessStatus, RemoteWriteStatus, ResourcePairStatus,
        ScrapeStatus, StatusSnapshot, Totals, UiStatus,
    };

    #[test]
    fn escapes_prometheus_labels() {
        assert_eq!(escape_label("a\\b\"c\nd"), "a\\\\b\\\"c\\nd");
    }

    fn snapshot_with_data() -> StatusSnapshot {
        StatusSnapshot {
            version: "1.2.3".into(),
            ready: true,
            uptime_seconds: 42,
            resource_pairs: vec![ResourcePairStatus {
                name: "service\"monitor\nedge".into(),
                prometheus_available: true,
                victoria_available: false,
                prometheus_count: 3,
                victoria_count: 1,
                native_count: 1,
                converted_count: 0,
                prometheus_objects: vec![CrdObjectStatus {
                    namespace: "default".into(),
                    name: "payments".into(),
                    converted: false,
                    estimated_memory_bytes: 4096,
                }],
                victoria_objects: Vec::new(),
            }],
            totals: Totals {
                crd_objects: 4,
                crd_pairs: 1,
                metric_series: Some(123),
            },
            process: ProcessStatus {
                cpu_percent: 12.3456,
                memory_bytes: 8192,
            },
            scrape: ScrapeStatus {
                enabled: true,
                targets: 10,
                healthy_targets: 9,
                samples: 1000,
                errors: 1,
                last_scrape_at: Some("2026-07-25T00:00:00Z".into()),
                last_error: Some("one target failed".into()),
            },
            crd_metric_cardinality: Vec::new(),
            counters: CountersStatus {
                reconciliations: 8,
                patches_victoria: 5,
                patches_prometheus: 3,
                errors: 1,
            },
            remote_write: RemoteWriteStatus {
                enabled: true,
                url: Some("[configured]".into()),
                last_publish_at: Some("2026-07-25T00:00:00Z".into()),
                last_series: Some(77),
                last_samples: Some(88),
                last_error: None,
            },
            ui: UiStatus {
                enabled: true,
                address: ":7070".into(),
                path: "/ui/".into(),
            },
        }
    }

    #[test]
    fn renders_help_types_values_labels_and_optional_remote_write_metrics() {
        let output = render(&snapshot_with_data());

        assert!(output.contains(
            "# HELP metrics_agent_ready Whether informer caches are synchronized.\n# TYPE metrics_agent_ready gauge\nmetrics_agent_ready 1\n"
        ));
        assert!(output.contains(
            "# HELP metrics_agent_precedence_reconciliations_total Total scrape precedence reconciliations."
        ));
        assert!(output.contains("metrics_agent_patches_total{source=\"victoriametrics\"} 5\n"));
        assert!(output.contains("metrics_agent_patches_total{source=\"prometheus\"} 3\n"));
        assert!(output.contains("metrics_agent_process_cpu_percent 12.346\n"));
        assert!(output.contains("metrics_agent_scrape_targets 10\n"));
        assert!(output.contains("metrics_agent_scrape_errors 1\n"));
        assert!(
            output.contains("resource_pair=\"service\\\"monitor\\nedge\",source=\"prometheus\"")
        );
        assert!(output.contains("metrics_agent_remote_write_series 123\n"));
        assert!(output.contains("metrics_agent_remote_write_samples 88\n"));
        assert!(output.contains("metrics_agent_remote_write_enabled 1\n"));
        assert!(output.contains("metrics_agent_remote_write_up 1\n"));
    }

    #[test]
    fn renders_empty_and_failed_remote_write_states_without_optional_values() {
        let mut snapshot = snapshot_with_data();
        snapshot.ready = false;
        snapshot.resource_pairs.clear();
        snapshot.totals.metric_series = None;
        snapshot.remote_write = RemoteWriteStatus {
            enabled: true,
            url: Some("[configured]".into()),
            last_publish_at: Some("2026-07-25T00:00:00Z".into()),
            last_series: None,
            last_samples: None,
            last_error: Some("connection refused".into()),
        };

        let output = render(&snapshot);
        assert!(output.contains("metrics_agent_ready 0\n"));
        assert!(output.contains("metrics_agent_remote_write_enabled 1\n"));
        assert!(output.contains("metrics_agent_remote_write_up 0\n"));
        assert!(!output.contains("metrics_agent_remote_write_series "));
        assert!(!output.contains("metrics_agent_remote_write_samples "));
        assert!(!output.contains("metrics_agent_crd_available{"));
    }
}
