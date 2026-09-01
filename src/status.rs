use std::{
    collections::HashMap,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use serde::Serialize;
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::sync::{Mutex, RwLock};

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ResourcePairStatus {
    pub name: String,
    pub prometheus_available: bool,
    pub victoria_available: bool,
    pub prometheus_count: u64,
    pub victoria_count: u64,
    pub native_count: u64,
    pub converted_count: u64,
    pub prometheus_objects: Vec<CrdObjectStatus>,
    pub victoria_objects: Vec<CrdObjectStatus>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CrdObjectStatus {
    pub namespace: String,
    pub name: String,
    pub converted: bool,
    pub estimated_memory_bytes: u64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Totals {
    pub crd_objects: u64,
    pub crd_pairs: u64,
    pub metric_series: Option<u64>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ProcessStatus {
    pub cpu_percent: f64,
    pub memory_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, PartialEq)]
pub struct ScrapeStatus {
    pub enabled: bool,
    pub targets: u64,
    pub healthy_targets: u64,
    pub samples: u64,
    pub errors: u64,
    pub duration_ms: u64,
    pub discovery_duration_ms: u64,
    pub discovery_cache_hit: bool,
    pub last_scrape_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct MetricCardinality {
    pub name: String,
    pub series: u64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CrdMetricStatus {
    pub key: String,
    pub pair: String,
    pub source: String,
    pub namespace: String,
    pub name: String,
    pub samples: u64,
    pub total_series: u64,
    pub top_metrics: Vec<MetricCardinality>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CountersStatus {
    pub reconciliations: u64,
    pub patches_victoria: u64,
    pub patches_prometheus: u64,
    pub errors: u64,
}

#[derive(Clone, Debug, Default, Serialize, PartialEq)]
pub struct RemoteWriteStatus {
    pub enabled: bool,
    pub url: Option<String>,
    pub last_publish_at: Option<String>,
    pub last_series: Option<u64>,
    pub last_samples: Option<u64>,
    pub last_duration_ms: Option<u64>,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct StatusSnapshot {
    pub version: String,
    pub ready: bool,
    pub uptime_seconds: u64,
    pub resource_pairs: Vec<ResourcePairStatus>,
    pub totals: Totals,
    pub process: ProcessStatus,
    pub scrape: ScrapeStatus,
    pub crd_metric_cardinality: Vec<CrdMetricStatus>,
    pub counters: CountersStatus,
    pub remote_write: RemoteWriteStatus,
    pub ui: UiStatus,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct UiStatus {
    pub enabled: bool,
    pub address: String,
    pub path: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct MetricExample {
    pub name: &'static str,
    pub description: &'static str,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct MetricsSummary {
    pub status: StatusSnapshot,
    pub metric_examples: Vec<MetricExample>,
}

#[derive(Default)]
struct PairObjects {
    prometheus: HashMap<String, ObjectState>,
    victoria: HashMap<String, ObjectState>,
}

#[derive(Clone, Copy, Default)]
struct ObjectState {
    converted: bool,
    estimated_memory_bytes: u64,
}

#[derive(Default)]
struct CrdMetricAccumulator {
    key: String,
    pair: String,
    source: String,
    namespace: String,
    name: String,
    samples: u64,
    counts: HashMap<String, u64>,
}

impl CrdMetricAccumulator {
    fn snapshot(&self) -> CrdMetricStatus {
        let mut top_metrics = self
            .counts
            .iter()
            .map(|(name, series)| MetricCardinality {
                name: name.clone(),
                series: *series,
            })
            .collect::<Vec<_>>();
        top_metrics.sort_by(|left, right| {
            right
                .series
                .cmp(&left.series)
                .then_with(|| left.name.cmp(&right.name))
        });
        top_metrics.truncate(15);
        CrdMetricStatus {
            key: self.key.clone(),
            pair: self.pair.clone(),
            source: self.source.clone(),
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            samples: self.samples,
            total_series: self.counts.values().sum(),
            top_metrics,
        }
    }
}

#[derive(Default)]
struct MutableStatus {
    ready: bool,
    pairs: Vec<ResourcePairStatus>,
    objects: Vec<PairObjects>,
    remote_write: RemoteWriteStatus,
    scrape: ScrapeStatus,
    crd_metrics: HashMap<String, CrdMetricAccumulator>,
}

#[derive(Default)]
pub struct Counters {
    pub reconciliations: AtomicU64,
    pub patches_victoria: AtomicU64,
    pub patches_prometheus: AtomicU64,
    pub errors: AtomicU64,
}

pub struct StatusStore {
    version: String,
    started: Instant,
    ui: UiStatus,
    mutable: RwLock<MutableStatus>,
    process: Mutex<System>,
    pub counters: Counters,
}

impl StatusStore {
    pub fn new(
        version: impl Into<String>,
        ui_enabled: bool,
        ui_address: impl Into<String>,
        ui_path: impl Into<String>,
        remote_write_url: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            version: version.into(),
            started: Instant::now(),
            ui: UiStatus {
                enabled: ui_enabled,
                address: ui_address.into(),
                path: ui_path.into(),
            },
            mutable: RwLock::new(MutableStatus {
                remote_write: RemoteWriteStatus {
                    enabled: remote_write_url.is_some(),
                    url: remote_write_url.map(|_| "[configured]".to_string()),
                    last_publish_at: None,
                    last_series: None,
                    last_samples: None,
                    last_duration_ms: None,
                    last_error: None,
                },
                scrape: ScrapeStatus::default(),
                ..Default::default()
            }),
            process: Mutex::new(System::new()),
            counters: Counters::default(),
        })
    }

    pub async fn initialize_pairs(&self, names: &[&str]) {
        let mut state = self.mutable.write().await;
        state.pairs = names
            .iter()
            .map(|name| ResourcePairStatus {
                name: (*name).to_string(),
                prometheus_available: false,
                victoria_available: false,
                prometheus_count: 0,
                victoria_count: 0,
                native_count: 0,
                converted_count: 0,
                prometheus_objects: Vec::new(),
                victoria_objects: Vec::new(),
            })
            .collect();
        state.objects = names.iter().map(|_| PairObjects::default()).collect();
    }

    pub async fn set_ready(&self, ready: bool) {
        self.mutable.write().await.ready = ready;
    }

    pub async fn set_source_available(&self, pair: usize, prometheus: bool, available: bool) {
        let mut state = self.mutable.write().await;
        if let Some(status) = state.pairs.get_mut(pair) {
            if prometheus {
                status.prometheus_available = available;
            } else {
                status.victoria_available = available;
            }
        }
    }

    pub async fn replace_objects(
        &self,
        pair: usize,
        prometheus: bool,
        objects: impl IntoIterator<Item = (String, bool, u64)>,
    ) {
        let mut state = self.mutable.write().await;
        let Some(pair_objects) = state.objects.get_mut(pair) else {
            return;
        };
        let target = if prometheus {
            &mut pair_objects.prometheus
        } else {
            &mut pair_objects.victoria
        };
        target.clear();
        target.extend(
            objects
                .into_iter()
                .map(|(key, converted, estimated_memory_bytes)| {
                    (
                        key,
                        ObjectState {
                            converted,
                            estimated_memory_bytes,
                        },
                    )
                }),
        );
        Self::refresh_pair(&mut state, pair);
    }

    pub async fn upsert_object(
        &self,
        pair: usize,
        prometheus: bool,
        key: String,
        converted: bool,
        estimated_memory_bytes: u64,
    ) {
        let mut state = self.mutable.write().await;
        let Some(pair_objects) = state.objects.get_mut(pair) else {
            return;
        };
        let target = if prometheus {
            &mut pair_objects.prometheus
        } else {
            &mut pair_objects.victoria
        };
        target.insert(
            key,
            ObjectState {
                converted,
                estimated_memory_bytes,
            },
        );
        Self::refresh_pair(&mut state, pair);
    }

    pub async fn remove_object(&self, pair: usize, prometheus: bool, key: &str) {
        let mut state = self.mutable.write().await;
        let Some(pair_objects) = state.objects.get_mut(pair) else {
            return;
        };
        let target = if prometheus {
            &mut pair_objects.prometheus
        } else {
            &mut pair_objects.victoria
        };
        target.remove(key);
        Self::refresh_pair(&mut state, pair);
    }

    fn refresh_pair(state: &mut MutableStatus, pair: usize) {
        let Some(objects) = state.objects.get(pair) else {
            return;
        };
        let Some(status) = state.pairs.get_mut(pair) else {
            return;
        };
        status.prometheus_count = objects.prometheus.len() as u64;
        status.victoria_count = objects.victoria.len() as u64;
        status.converted_count = objects
            .victoria
            .values()
            .filter(|object| object.converted)
            .count() as u64;
        status.native_count = status.victoria_count.saturating_sub(status.converted_count);
        status.prometheus_objects = Self::object_statuses(&objects.prometheus);
        status.victoria_objects = Self::object_statuses(&objects.victoria);
    }

    fn object_statuses(objects: &HashMap<String, ObjectState>) -> Vec<CrdObjectStatus> {
        let mut result = objects
            .iter()
            .map(|(key, object)| {
                let (namespace, name) = key.split_once('/').unwrap_or(("", key.as_str()));
                CrdObjectStatus {
                    namespace: namespace.to_string(),
                    name: name.to_string(),
                    converted: object.converted,
                    estimated_memory_bytes: object.estimated_memory_bytes,
                }
            })
            .collect::<Vec<_>>();
        result.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then_with(|| left.name.cmp(&right.name))
        });
        result
    }

    pub async fn set_remote_write_result(
        &self,
        series: Option<u64>,
        samples: Option<u64>,
        timestamp: String,
        duration_ms: u64,
        error: Option<String>,
    ) {
        let mut state = self.mutable.write().await;
        state.remote_write.last_series = series;
        state.remote_write.last_samples = samples;
        state.remote_write.last_publish_at = Some(timestamp);
        state.remote_write.last_duration_ms = Some(duration_ms);
        state.remote_write.last_error = error;
    }

    pub async fn set_scrape_status(&self, scrape: ScrapeStatus) {
        self.mutable.write().await.scrape = scrape;
    }

    pub async fn begin_scrape(&self) {
        self.mutable.write().await.crd_metrics.clear();
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn record_crd_metrics(
        &self,
        key: String,
        pair: String,
        source: String,
        namespace: String,
        name: String,
        samples: u64,
        counts: impl IntoIterator<Item = MetricCardinality>,
    ) {
        let mut state = self.mutable.write().await;
        let entry = state
            .crd_metrics
            .entry(key.clone())
            .or_insert_with(|| CrdMetricAccumulator {
                key,
                pair,
                source,
                namespace,
                name,
                ..Default::default()
            });
        entry.samples += samples;
        for metric in counts {
            *entry.counts.entry(metric.name).or_default() += metric.series;
        }
    }

    pub async fn snapshot(&self) -> StatusSnapshot {
        let state = self.mutable.read().await;
        let pid = Pid::from_u32(std::process::id());
        let mut process = self.process.lock().await;
        process.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        let (cpu_percent, memory_bytes) = process
            .process(pid)
            .map(|process| (f64::from(process.cpu_usage()), process.memory()))
            .unwrap_or((0.0, 0));
        let mut crd_metric_cardinality = state
            .crd_metrics
            .values()
            .map(CrdMetricAccumulator::snapshot)
            .collect::<Vec<_>>();
        crd_metric_cardinality.sort_by(|left, right| left.key.cmp(&right.key));
        StatusSnapshot {
            version: self.version.clone(),
            ready: state.ready,
            uptime_seconds: self.started.elapsed().as_secs(),
            resource_pairs: state.pairs.clone(),
            totals: Totals {
                crd_objects: state
                    .pairs
                    .iter()
                    .map(|pair| pair.prometheus_count + pair.victoria_count)
                    .sum(),
                crd_pairs: state
                    .pairs
                    .iter()
                    .filter(|pair| pair.prometheus_available || pair.victoria_available)
                    .count() as u64,
                metric_series: state.remote_write.last_series,
            },
            process: ProcessStatus {
                cpu_percent,
                memory_bytes,
            },
            scrape: state.scrape.clone(),
            crd_metric_cardinality,
            counters: CountersStatus {
                reconciliations: self.counters.reconciliations.load(Ordering::Relaxed),
                patches_victoria: self.counters.patches_victoria.load(Ordering::Relaxed),
                patches_prometheus: self.counters.patches_prometheus.load(Ordering::Relaxed),
                errors: self.counters.errors.load(Ordering::Relaxed),
            },
            remote_write: state.remote_write.clone(),
            ui: self.ui.clone(),
        }
    }

    pub async fn metrics_summary(&self) -> MetricsSummary {
        MetricsSummary {
            status: self.snapshot().await,
            metric_examples: vec![
                MetricExample {
                    name: "metrics_agent_precedence_reconciliations_total",
                    description: "Total precedence reconciliation attempts",
                },
                MetricExample {
                    name: "metrics_agent_observed_crd_objects",
                    description: "Observed scrape CRD objects by resource pair and source",
                },
                MetricExample {
                    name: "metrics_agent_process_resident_memory_bytes",
                    description: "Resident memory used by the controller",
                },
                MetricExample {
                    name: "metrics_agent_remote_write_series",
                    description: "Number of agent metric series in the last Rush remote-write payload",
                },
            ],
        }
    }

    pub fn uptime(&self) -> Duration {
        self.started.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::{MetricCardinality, ScrapeStatus, StatusStore};

    #[tokio::test]
    async fn aggregates_native_and_converted_objects() {
        let store = StatusStore::new("test", false, ":8080", "/ui/", None);
        store.initialize_pairs(&["service-monitor"]).await;
        store.set_source_available(0, false, true).await;
        store
            .replace_objects(
                0,
                false,
                [
                    ("native".into(), false, 1024),
                    ("converted".into(), true, 2048),
                ],
            )
            .await;
        store
            .replace_objects(0, true, [("prom".into(), false, 512)])
            .await;
        let snapshot = store.snapshot().await;
        assert_eq!(snapshot.resource_pairs[0].native_count, 1);
        assert_eq!(snapshot.resource_pairs[0].converted_count, 1);
        assert_eq!(snapshot.totals.crd_objects, 3);
        assert_eq!(snapshot.resource_pairs[0].victoria_objects.len(), 2);
        assert_eq!(
            snapshot.resource_pairs[0].victoria_objects[1].name,
            "native"
        );
        assert_eq!(
            snapshot.resource_pairs[0].victoria_objects[1].estimated_memory_bytes,
            1024
        );
    }

    #[tokio::test]
    async fn tracks_readiness_source_transitions_and_empty_objects() {
        let store = StatusStore::new("test", true, ":7070", "/control-room/", None);

        let initial = store.snapshot().await;
        assert!(!initial.ready);
        assert!(initial.resource_pairs.is_empty());
        assert_eq!(initial.totals.crd_objects, 0);
        assert_eq!(initial.totals.crd_pairs, 0);

        store.initialize_pairs(&["service-monitor"]).await;
        store.set_source_available(0, true, true).await;
        store.set_source_available(0, false, false).await;
        store.set_ready(true).await;
        let ready = store.snapshot().await;
        assert!(ready.ready);
        assert!(ready.resource_pairs[0].prometheus_available);
        assert!(!ready.resource_pairs[0].victoria_available);
        assert_eq!(ready.totals.crd_pairs, 1);

        store.replace_objects(0, true, std::iter::empty()).await;
        store.remove_object(99, true, "missing").await;
        store.set_source_available(99, true, true).await;
        store.set_ready(false).await;
        assert!(!store.snapshot().await.ready);
    }

    #[tokio::test]
    async fn aggregates_crd_cardinality_across_targets_and_caps_top_fifteen() {
        let store = StatusStore::new("test", false, ":7070", "/ui/", None);
        store.begin_scrape().await;

        let first_batch = (0..16).map(|index| MetricCardinality {
            name: format!("metric_{index:02}"),
            series: index as u64 + 1,
        });
        store
            .record_crd_metrics(
                "victoria:service-monitor:default/payments".into(),
                "service-monitor".into(),
                "victoria".into(),
                "default".into(),
                "payments".into(),
                16,
                first_batch,
            )
            .await;
        store
            .record_crd_metrics(
                "victoria:service-monitor:default/payments".into(),
                "service-monitor".into(),
                "victoria".into(),
                "default".into(),
                "payments".into(),
                3,
                [
                    MetricCardinality {
                        name: "metric_15".into(),
                        series: 4,
                    },
                    MetricCardinality {
                        name: "metric_extra".into(),
                        series: 100,
                    },
                ],
            )
            .await;

        let snapshot = store.snapshot().await;
        let cardinality = &snapshot.crd_metric_cardinality;
        assert_eq!(cardinality.len(), 1);
        assert_eq!(cardinality[0].samples, 19);
        assert_eq!(cardinality[0].total_series, 240);
        assert_eq!(cardinality[0].top_metrics.len(), 15);
        assert_eq!(cardinality[0].top_metrics[0].name, "metric_extra");
        assert_eq!(cardinality[0].top_metrics[0].series, 100);
        assert!(
            !cardinality
                .iter()
                .flat_map(|entry| entry.top_metrics.iter())
                .any(|metric| metric.name == "metric_00")
        );
    }

    #[tokio::test]
    async fn begins_a_new_scrape_by_clearing_previous_cardinality() {
        let store = StatusStore::new("test", false, ":7070", "/ui/", None);
        store
            .record_crd_metrics(
                "prometheus:probe:ns/check".into(),
                "probe".into(),
                "prometheus".into(),
                "ns".into(),
                "check".into(),
                1,
                [MetricCardinality {
                    name: "up".into(),
                    series: 1,
                }],
            )
            .await;
        assert_eq!(store.snapshot().await.crd_metric_cardinality.len(), 1);

        store.begin_scrape().await;
        assert!(store.snapshot().await.crd_metric_cardinality.is_empty());
    }

    #[tokio::test]
    async fn concurrent_cardinality_updates_do_not_lose_counts() {
        let store = StatusStore::new("test", false, ":7070", "/ui/", None);
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                store
                    .record_crd_metrics(
                        "victoria:service-monitor:default/api".into(),
                        "service-monitor".into(),
                        "victoria".into(),
                        "default".into(),
                        "api".into(),
                        2,
                        [MetricCardinality {
                            name: "requests_total".into(),
                            series: 3,
                        }],
                    )
                    .await;
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        let snapshot = store.snapshot().await;
        let cardinality = &snapshot.crd_metric_cardinality[0];
        assert_eq!(cardinality.samples, 64);
        assert_eq!(cardinality.total_series, 96);
        assert_eq!(cardinality.top_metrics[0].series, 96);
    }

    #[tokio::test]
    async fn redacts_remote_write_configuration_and_preserves_failure_state() {
        let store = StatusStore::new(
            "test",
            false,
            ":7070",
            "/ui/",
            Some("https://rush.example/write?token=secret".into()),
        );
        let configured = store.snapshot().await;
        assert!(configured.remote_write.enabled);
        assert_eq!(configured.remote_write.url.as_deref(), Some("[configured]"));

        store
            .set_remote_write_result(
                None,
                None,
                "2026-07-25T00:00:00Z".into(),
                125,
                Some("connection refused".into()),
            )
            .await;
        let failed = store.snapshot().await;
        assert_eq!(failed.remote_write.last_series, None);
        assert_eq!(failed.remote_write.last_samples, None);
        assert_eq!(
            failed.remote_write.last_error.as_deref(),
            Some("connection refused")
        );
        assert_eq!(failed.remote_write.url.as_deref(), Some("[configured]"));
    }

    #[tokio::test]
    async fn exposes_scrape_errors_and_metrics_summary_for_empty_state() {
        let store = StatusStore::new("test", false, ":7070", "/ui/", None);
        store
            .set_scrape_status(ScrapeStatus {
                enabled: true,
                targets: 2,
                healthy_targets: 1,
                samples: 0,
                errors: 1,
                duration_ms: 10_000,
                discovery_duration_ms: 125,
                discovery_cache_hit: true,
                last_scrape_at: Some("2026-07-25T00:00:00Z".into()),
                last_error: Some("target timed out".into()),
            })
            .await;

        let summary = store.metrics_summary().await;
        assert_eq!(summary.status.scrape.targets, 2);
        assert_eq!(summary.status.scrape.healthy_targets, 1);
        assert_eq!(summary.status.scrape.errors, 1);
        assert_eq!(
            summary.status.scrape.last_error.as_deref(),
            Some("target timed out")
        );
        assert!(!summary.metric_examples.is_empty());
    }
}
