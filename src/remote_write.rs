use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use prost::Message;
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE};

use crate::{metrics, status::StatusSnapshot};

#[derive(Clone, PartialEq, Message)]
struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    timeseries: Vec<TimeSeries>,
    #[prost(message, repeated, tag = "3")]
    metadata: Vec<MetricMetadata>,
}

#[derive(Clone, PartialEq, Message)]
struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    samples: Vec<Sample>,
}

#[derive(Clone, PartialEq, Message)]
struct Label {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, Message)]
struct Sample {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(int64, tag = "2")]
    timestamp: i64,
}

#[derive(Clone, PartialEq, Message)]
struct MetricMetadata {
    #[prost(enumeration = "MetricType", tag = "1")]
    r#type: i32,
    #[prost(string, tag = "2")]
    metric_family_name: String,
    #[prost(string, tag = "4")]
    help: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum MetricType {
    Unknown = 0,
    Counter = 1,
    Gauge = 2,
}

/// A metric sample collected from a Kubernetes scrape target.
#[derive(Clone, Debug)]
pub struct CollectedMetric {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub value: f64,
    pub timestamp: i64,
    pub help: String,
    pub metric_type: MetricType,
}

#[derive(Clone, Copy, Debug)]
pub struct ScrapeLimits {
    pub max_response_bytes: usize,
    pub max_samples_per_target: usize,
    pub max_labels_per_sample: usize,
    pub max_label_name_bytes: usize,
    pub max_label_value_bytes: usize,
    pub max_metric_name_bytes: usize,
    pub max_line_bytes: usize,
}

impl Default for ScrapeLimits {
    fn default() -> Self {
        Self {
            max_response_bytes: 4_194_304,
            max_samples_per_target: 50_000,
            max_labels_per_sample: 64,
            max_label_name_bytes: 256,
            max_label_value_bytes: 4_096,
            max_metric_name_bytes: 1_024,
            max_line_bytes: 65_536,
        }
    }
}

/// Bounded scrape-to-write messages. Keeping the samples in the channel
/// instead of a shared whole-cycle vector prevents the agent from retaining
/// an entire scrape's worth of parsed samples between publishes.
pub enum ScrapeMessage {
    Start {
        targets: usize,
    },
    Batch(Vec<CollectedMetric>),
    Complete {
        targets: usize,
        healthy_targets: usize,
        samples: u64,
        error: Option<String>,
    },
}

#[derive(Debug)]
pub struct PublishResult {
    pub series: u64,
    pub samples: u64,
}

/// Converts the agent's own Prometheus exposition into Rush's remote-write
/// protobuf and sends it directly to the query API. This keeps self-metrics
/// independent of an external scrape agent or a ServiceMonitor.
pub async fn publish(
    client: &reqwest::Client,
    url: &str,
    token: Option<&str>,
    tenant: Option<&str>,
    snapshot: &StatusSnapshot,
    scraped: &[CollectedMetric],
    extra_labels: &BTreeMap<String, String>,
) -> Result<PublishResult> {
    publish_inner(
        client,
        url,
        Some(snapshot),
        scraped.to_vec(),
        PublishOptions {
            token,
            tenant,
            extra_labels,
            include_self: true,
        },
    )
    .await
}

struct PublishOptions<'a> {
    token: Option<&'a str>,
    tenant: Option<&'a str>,
    extra_labels: &'a BTreeMap<String, String>,
    include_self: bool,
}

async fn publish_inner(
    client: &reqwest::Client,
    url: &str,
    snapshot: Option<&StatusSnapshot>,
    scraped: Vec<CollectedMetric>,
    options: PublishOptions<'_>,
) -> Result<PublishResult> {
    let snapshot = snapshot.cloned();
    let extra_labels = options.extra_labels.clone();
    let include_self = options.include_self;
    let (payload, compressed) = tokio::task::spawn_blocking(move || {
        let payload = encode(snapshot.as_ref(), &scraped, &extra_labels, include_self)?;
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&payload.bytes)
            .context("compress remote-write payload")?;
        Ok::<_, anyhow::Error>((payload, compressed))
    })
    .await
    .context("join remote-write encoding task")??;

    let mut request = client
        .post(url)
        .header(CONTENT_TYPE, "application/x-protobuf")
        .header(CONTENT_ENCODING, "snappy")
        .header("X-Prometheus-Remote-Write-Version", "0.1.0")
        .body(compressed);
    if let Some(token) = options.token.filter(|token| !token.is_empty()) {
        request = request.bearer_auth(token);
    }
    if let Some(tenant) = options.tenant.filter(|tenant| !tenant.is_empty()) {
        request = request.header("X-Rush-Tenant", tenant);
    }
    let response = request.send().await.context("send remote-write payload")?;
    if !response.status().is_success() {
        let status = response.status();
        return Err(anyhow!("Rush rejected remote-write payload: HTTP {status}"));
    }

    Ok(PublishResult {
        series: payload.series,
        samples: payload.samples,
    })
}

struct EncodedPayload {
    bytes: Vec<u8>,
    series: u64,
    samples: u64,
}

fn encode(
    snapshot: Option<&StatusSnapshot>,
    scraped: &[CollectedMetric],
    extra_labels: &BTreeMap<String, String>,
    include_self: bool,
) -> Result<EncodedPayload> {
    let mut helps = HashMap::new();
    let mut types = HashMap::new();
    let mut series = Vec::new();

    if include_self {
        let snapshot = snapshot.context("self metrics require a status snapshot")?;
        let exposition = metrics::render(snapshot);
        for line in exposition.lines() {
            if let Some(help) = line.strip_prefix("# HELP ") {
                let mut fields = help.splitn(2, ' ');
                if let (Some(name), Some(description)) = (fields.next(), fields.next()) {
                    helps.insert(name.to_string(), description.to_string());
                }
                continue;
            }
            if let Some(kind) = line.strip_prefix("# TYPE ") {
                let mut fields = kind.split_whitespace();
                if let (Some(name), Some(kind)) = (fields.next(), fields.next()) {
                    types.insert(name.to_string(), metric_type(kind));
                }
                continue;
            }
            if line.starts_with('#') || line.trim().is_empty() {
                continue;
            }
            let Some((metric, raw_value)) = line.split_once(char::is_whitespace) else {
                continue;
            };
            let value = raw_value.trim().parse::<f64>().with_context(|| {
                format!("parse Prometheus value in remote-write metric {line:?}")
            })?;
            let (name, mut labels) = parse_metric(metric)?;
            // Prometheus remote write carries the metric family in the reserved
            // `__name__` label. The query-api uses that label to populate
            // MetricName; without it the payload is accepted but every sample is
            // discarded as nameless.
            labels.push(Label {
                name: "__name__".to_string(),
                value: name.to_string(),
            });
            if !labels.iter().any(|label| label.name == "job") {
                labels.push(Label {
                    name: "job".to_string(),
                    value: "metrics-agent".to_string(),
                });
            }
            if !labels.iter().any(|label| label.name == "service") {
                labels.push(Label {
                    name: "service".to_string(),
                    value: "metrics-agent".to_string(),
                });
            }
            merge_extra_labels(&mut labels, extra_labels);
            labels.sort_by(|left, right| left.name.cmp(&right.name));
            series.push((
                name.to_string(),
                TimeSeries {
                    labels,
                    samples: vec![Sample {
                        value,
                        timestamp: Utc::now().timestamp_millis(),
                    }],
                },
            ));
        }
    }

    for metric in scraped {
        let mut labels = metric
            .labels
            .iter()
            .filter(|(name, _)| name != "__name__")
            .map(|(name, value)| Label {
                name: name.clone(),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        labels.push(Label {
            name: "__name__".to_string(),
            value: metric.name.clone(),
        });
        merge_extra_labels(&mut labels, extra_labels);
        labels.sort_by(|left, right| left.name.cmp(&right.name));
        helps.insert(metric.name.clone(), metric.help.clone());
        types.insert(metric.name.clone(), metric.metric_type as i32);
        series.push((
            metric.name.clone(),
            TimeSeries {
                labels,
                samples: vec![Sample {
                    value: metric.value,
                    timestamp: metric.timestamp,
                }],
            },
        ));
    }

    let mut metadata = BTreeMap::new();
    for (name, _) in &series {
        metadata
            .entry(name.clone())
            .or_insert_with(|| MetricMetadata {
                r#type: types.get(name).copied().unwrap_or(MetricType::Gauge as i32),
                metric_family_name: name.clone(),
                help: helps.get(name).cloned().unwrap_or_default(),
            });
    }
    let samples = series
        .iter()
        .map(|(_, time_series)| time_series.samples.len() as u64)
        .sum();
    let request = WriteRequest {
        timeseries: series.into_iter().map(|(_, series)| series).collect(),
        metadata: metadata.into_values().collect(),
    };
    Ok(EncodedPayload {
        bytes: request.encode_to_vec(),
        series: request.timeseries.len() as u64,
        samples,
    })
}

/// Apply deployment-wide labels to every outgoing series. Configured values
/// intentionally replace same-named target/exporter labels so a declaration
/// such as `env=dev` has one consistent value across the entire payload.
fn merge_extra_labels(labels: &mut Vec<Label>, extra_labels: &BTreeMap<String, String>) {
    for (name, value) in extra_labels {
        if let Some(existing) = labels.iter_mut().find(|label| label.name == *name) {
            existing.value.clone_from(value);
        } else {
            labels.push(Label {
                name: name.clone(),
                value: value.clone(),
            });
        }
    }
}

fn metric_type(kind: &str) -> i32 {
    match kind {
        "counter" => MetricType::Counter as i32,
        _ => MetricType::Gauge as i32,
    }
}

fn parse_metric(metric: &str) -> Result<(&str, Vec<Label>)> {
    let Some((name, raw_labels)) = metric.split_once('{') else {
        return Ok((metric, Vec::new()));
    };
    let raw_labels = raw_labels
        .strip_suffix('}')
        .context("remote-write metric labels are missing a closing brace")?;
    Ok((name, parse_labels(raw_labels)?))
}

/// Parse Prometheus exposition text returned by a scrape target.
pub fn parse_prometheus_text(
    body: &str,
    target_labels: &[(String, String)],
    default_timestamp: i64,
    limits: ScrapeLimits,
) -> Result<Vec<CollectedMetric>> {
    let mut parser = PrometheusTextParser::new(target_labels, default_timestamp, limits)?;
    for line in body.lines() {
        parser.push_line(line)?;
    }
    Ok(parser.finish())
}

/// Incremental Prometheus text parser used by the HTTP scraper. It retains
/// parsed samples and metadata, but never the complete response body.
pub struct PrometheusTextParser {
    target_labels: Vec<(String, String)>,
    default_timestamp: i64,
    limits: ScrapeLimits,
    helps: HashMap<String, String>,
    types: HashMap<String, i32>,
    samples: Vec<CollectedMetric>,
}

impl PrometheusTextParser {
    pub fn new(
        target_labels: &[(String, String)],
        default_timestamp: i64,
        limits: ScrapeLimits,
    ) -> Result<Self> {
        validate_target_labels(target_labels, limits)?;
        Ok(Self {
            target_labels: target_labels.to_vec(),
            default_timestamp,
            limits,
            helps: HashMap::new(),
            types: HashMap::new(),
            samples: Vec::new(),
        })
    }

    pub fn push_line(&mut self, line: &str) -> Result<()> {
        if line.len() > self.limits.max_line_bytes {
            bail!("Prometheus exposition line exceeds the configured byte limit");
        }
        if let Some(help) = line.strip_prefix("# HELP ") {
            let mut fields = help.splitn(2, ' ');
            if let (Some(name), Some(description)) = (fields.next(), fields.next()) {
                validate_metric_name(name, self.limits)?;
                if description.len() > self.limits.max_label_value_bytes {
                    bail!("Prometheus HELP text exceeds the configured byte limit");
                }
                self.helps.insert(name.to_string(), description.to_string());
            }
            return Ok(());
        }
        if let Some(kind) = line.strip_prefix("# TYPE ") {
            let mut fields = kind.split_whitespace();
            if let (Some(name), Some(kind)) = (fields.next(), fields.next()) {
                validate_metric_name(name, self.limits)?;
                self.types.insert(name.to_string(), metric_type(kind));
            }
            return Ok(());
        }
        if line.starts_with('#') || line.trim().is_empty() {
            return Ok(());
        }
        let Some((metric, raw_value)) = line.split_once(char::is_whitespace) else {
            return Ok(());
        };
        let mut fields = raw_value.split_whitespace();
        let Ok(value) = fields.next().unwrap_or_default().parse::<f64>() else {
            return Ok(());
        };
        if !value.is_finite() {
            return Ok(());
        }
        let timestamp = fields
            .next()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(self.default_timestamp);
        let (name, labels) = parse_metric_bounded(metric, self.limits)?;
        if self.samples.len() >= self.limits.max_samples_per_target {
            bail!("scrape target exceeds the configured sample limit");
        }
        let mut merged = self.target_labels.clone();
        for label in labels {
            if let Some(existing) = merged.iter_mut().find(|(key, _)| key == &label.name) {
                existing.1 = label.value;
            } else {
                if merged.len() >= self.limits.max_labels_per_sample {
                    bail!("Prometheus sample exceeds the configured label-count limit");
                }
                merged.push((label.name, label.value));
            }
        }
        self.samples.push(CollectedMetric {
            name: name.to_string(),
            labels: merged,
            value,
            timestamp,
            help: String::new(),
            metric_type: MetricType::Gauge,
        });
        Ok(())
    }

    pub fn finish(mut self) -> Vec<CollectedMetric> {
        for sample in &mut self.samples {
            sample.help = self.helps.get(&sample.name).cloned().unwrap_or_default();
            sample.metric_type = self
                .types
                .get(&sample.name)
                .copied()
                .and_then(|value| MetricType::try_from(value).ok())
                .unwrap_or(MetricType::Gauge);
        }
        self.samples
    }
}

fn validate_target_labels(target_labels: &[(String, String)], limits: ScrapeLimits) -> Result<()> {
    if target_labels.len() > limits.max_labels_per_sample {
        bail!("scrape target labels exceed the configured label-count limit");
    }
    let mut names = HashSet::with_capacity(target_labels.len());
    for (name, value) in target_labels {
        if !names.insert(name) {
            bail!("scrape target labels contain a duplicate name");
        }
        if name.len() > limits.max_label_name_bytes {
            bail!("scrape target label name exceeds the configured byte limit");
        }
        if value.len() > limits.max_label_value_bytes {
            bail!("scrape target label value exceeds the configured byte limit");
        }
    }
    Ok(())
}

fn validate_metric_name(name: &str, limits: ScrapeLimits) -> Result<()> {
    if name.is_empty() {
        bail!("Prometheus metric name is empty");
    }
    if name.len() > limits.max_metric_name_bytes {
        bail!("Prometheus metric name exceeds the configured byte limit");
    }
    Ok(())
}

fn parse_metric_bounded(metric: &str, limits: ScrapeLimits) -> Result<(&str, Vec<Label>)> {
    let Some((name, raw_labels)) = metric.split_once('{') else {
        validate_metric_name(metric, limits)?;
        return Ok((metric, Vec::new()));
    };
    validate_metric_name(name, limits)?;
    let raw_labels = raw_labels
        .strip_suffix('}')
        .context("Prometheus metric labels are missing a closing brace")?;
    Ok((name, parse_labels_bounded(raw_labels, limits)?))
}

fn parse_labels(raw: &str) -> Result<Vec<Label>> {
    parse_labels_bounded(
        raw,
        ScrapeLimits {
            max_labels_per_sample: usize::MAX,
            max_label_name_bytes: usize::MAX,
            max_label_value_bytes: usize::MAX,
            ..ScrapeLimits::default()
        },
    )
}

fn parse_labels_bounded(raw: &str, limits: ScrapeLimits) -> Result<Vec<Label>> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let bytes = raw.as_bytes();
    let mut cursor = 0;
    let mut labels = Vec::new();
    let mut names = HashSet::new();
    while cursor < bytes.len() {
        while bytes.get(cursor) == Some(&b',') || bytes.get(cursor) == Some(&b' ') {
            cursor += 1;
        }
        let key_start = cursor;
        while cursor < bytes.len() && bytes[cursor] != b'=' {
            cursor += 1;
        }
        let key = raw[key_start..cursor].trim();
        if key.is_empty() || cursor >= bytes.len() {
            return Err(anyhow!("invalid Prometheus label set"));
        }
        if key.len() > limits.max_label_name_bytes {
            bail!("Prometheus label name exceeds the configured byte limit");
        }
        if !names.insert(key) {
            bail!("Prometheus sample contains a duplicate label name");
        }
        cursor += 1;
        if bytes.get(cursor) != Some(&b'\"') {
            return Err(anyhow!("Prometheus label value is not quoted"));
        }
        cursor += 1;
        let mut value = String::new();
        let mut closed = false;
        while cursor < bytes.len() {
            match bytes[cursor] {
                b'\"' => {
                    cursor += 1;
                    closed = true;
                    break;
                }
                b'\\' => {
                    if cursor + 1 >= bytes.len() {
                        bail!("Prometheus label value contains an invalid escape");
                    }
                    cursor += 1;
                    let character = match bytes[cursor] {
                        b'n' => '\n',
                        b'\\' => '\\',
                        b'\"' => '\"',
                        _ => bail!("Prometheus label value contains an invalid escape"),
                    };
                    if value.len() + character.len_utf8() > limits.max_label_value_bytes {
                        bail!("Prometheus label value exceeds the configured byte limit");
                    }
                    value.push(character);
                    cursor += 1;
                }
                _ => {
                    let character = raw[cursor..]
                        .chars()
                        .next()
                        .context("invalid UTF-8 boundary in Prometheus label value")?;
                    if value.len() + character.len_utf8() > limits.max_label_value_bytes {
                        bail!("Prometheus label value exceeds the configured byte limit");
                    }
                    value.push(character);
                    cursor += character.len_utf8();
                }
            }
        }
        if !closed {
            return Err(anyhow!("Prometheus label value is not closed"));
        }
        if labels.len() >= limits.max_labels_per_sample {
            bail!("Prometheus sample exceeds the configured label-count limit");
        }
        labels.push(Label {
            name: key.to_string(),
            value,
        });
        while bytes.get(cursor) == Some(&b' ') {
            cursor += 1;
        }
        if cursor < bytes.len() && bytes[cursor] != b',' {
            return Err(anyhow!("invalid separator in Prometheus label set"));
        }
        if bytes.get(cursor) == Some(&b',') {
            cursor += 1;
        }
    }
    Ok(labels)
}

#[allow(clippy::too_many_arguments)]
pub async fn run(
    client: reqwest::Client,
    status: Arc<crate::status::StatusStore>,
    mut scrape_receiver: tokio::sync::mpsc::Receiver<ScrapeMessage>,
    url: Option<String>,
    token: Option<String>,
    tenant: Option<String>,
    extra_labels: BTreeMap<String, String>,
    interval: std::time::Duration,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let mut interval = tokio::time::interval(interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cycle_active = false;
    let mut cycle_started: Option<std::time::Instant> = None;
    let mut cycle_sent_self = false;
    let mut cycle_series = 0;
    let mut cycle_samples = 0;
    let mut cycle_error: Option<anyhow::Error> = None;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            message = scrape_receiver.recv() => {
                let Some(message) = message else { return };
                match message {
                    ScrapeMessage::Start { .. } => {
                        cycle_active = true;
                        cycle_started = Some(std::time::Instant::now());
                        cycle_sent_self = false;
                        cycle_series = 0;
                        cycle_samples = 0;
                        cycle_error = None;
                    }
                    ScrapeMessage::Batch(metrics) => {
                        let Some(url) = url.as_deref() else { continue };
                        if cycle_error.is_some() { continue; }
                        let snapshot = if cycle_sent_self {
                            None
                        } else {
                            Some(status.snapshot().await)
                        };
                        match publish_inner(
                            &client,
                            url,
                            snapshot.as_ref(),
                            metrics,
                            PublishOptions {
                                token: token.as_deref(),
                                tenant: tenant.as_deref(),
                                extra_labels: &extra_labels,
                                include_self: !cycle_sent_self,
                            },
                        ).await {
                            Ok(result) => {
                                cycle_sent_self = true;
                                cycle_series += result.series;
                                cycle_samples += result.samples;
                            }
                            Err(error) => cycle_error = Some(error),
                        }
                    }
                    ScrapeMessage::Complete { targets: _, healthy_targets: _, samples: _, error } => {
                        if let Some(error) = error {
                            cycle_error = Some(anyhow!(error));
                        } else if let Some(url) = url.as_deref() {
                            // A scrape with no samples should still publish the
                            // agent's own health metrics.
                            if cycle_active && !cycle_sent_self && cycle_error.is_none() {
                                let snapshot = status.snapshot().await;
                                match publish_inner(
                                    &client,
                                    url,
                                    Some(&snapshot),
                                    Vec::new(),
                                    PublishOptions {
                                        token: token.as_deref(),
                                        tenant: tenant.as_deref(),
                                        extra_labels: &extra_labels,
                                        include_self: true,
                                    },
                                ).await {
                                    Ok(result) => {
                                        cycle_sent_self = true;
                                        cycle_series += result.series;
                                        cycle_samples += result.samples;
                                    }
                                    Err(error) => cycle_error = Some(error),
                                }
                            }
                        }
                        let timestamp = humantime::format_rfc3339_seconds(SystemTime::now()).to_string();
                        let duration_ms = cycle_started
                            .take()
                            .map(|started| started.elapsed().as_millis() as u64)
                            .unwrap_or(0);
                        match cycle_error.take() {
                            None if url.is_some() => status.set_remote_write_result(Some(cycle_series), Some(cycle_samples), timestamp, duration_ms, None).await,
                            Some(error) => status.set_remote_write_result(None, None, timestamp, duration_ms, Some(error.to_string())).await,
                            _ => {}
                        }
                        cycle_active = false;
                    }
                }
            }
            _ = interval.tick() => {
                if !cycle_active {
                    if let Some(url) = url.as_deref() {
                        let snapshot = status.snapshot().await;
                        // Once workload targets are active, their completed
                        // cycle is the meaningful "last payload". Do not
                        // overwrite that count with a small self-health-only
                        // heartbeat between scrape cycles.
                        if snapshot.scrape.enabled && snapshot.scrape.targets > 0 {
                            continue;
                        }
                        let timestamp = humantime::format_rfc3339_seconds(SystemTime::now()).to_string();
                        let started = std::time::Instant::now();
                        match publish_inner(
                            &client,
                            url,
                            Some(&snapshot),
                            Vec::new(),
                            PublishOptions {
                                token: token.as_deref(),
                                tenant: tenant.as_deref(),
                                extra_labels: &extra_labels,
                                include_self: true,
                            },
                        ).await {
                            Ok(result) => status.set_remote_write_result(Some(result.series), Some(result.samples), timestamp, started.elapsed().as_millis() as u64, None).await,
                            Err(error) => status.set_remote_write_result(None, None, timestamp, started.elapsed().as_millis() as u64, Some(error.to_string())).await,
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use prost::Message;

    use super::{
        CollectedMetric, MetricType, PrometheusTextParser, ScrapeLimits, WriteRequest, encode,
        parse_labels, parse_prometheus_text,
    };
    use crate::status::{
        CountersStatus, ProcessStatus, RemoteWriteStatus, ScrapeStatus, StatusSnapshot, Totals,
        UiStatus,
    };

    fn snapshot() -> StatusSnapshot {
        StatusSnapshot {
            version: "test".into(),
            ready: true,
            uptime_seconds: 7,
            resource_pairs: Vec::new(),
            totals: Totals {
                crd_objects: 0,
                crd_pairs: 0,
                metric_series: None,
            },
            process: ProcessStatus {
                cpu_percent: 0.0,
                memory_bytes: 0,
            },
            scrape: ScrapeStatus::default(),
            crd_metric_cardinality: Vec::new(),
            counters: CountersStatus {
                reconciliations: 0,
                patches_victoria: 0,
                patches_prometheus: 0,
                errors: 0,
            },
            remote_write: RemoteWriteStatus::default(),
            ui: UiStatus {
                enabled: false,
                address: "127.0.0.1:7070".into(),
                path: "/ui/".into(),
            },
        }
    }

    #[test]
    fn parses_escaped_labels() {
        let labels = parse_labels(r#"resource_pair="service-monitor",note="a\\b""#).unwrap();
        assert_eq!(labels[0].value, "service-monitor");
        assert_eq!(labels[1].value, "a\\b");
    }

    #[test]
    fn parses_scraped_samples_with_target_labels() {
        let samples = parse_prometheus_text(
            "# HELP http_requests_total Requests.\n# TYPE http_requests_total counter\nhttp_requests_total{code=\"200\"} 7 1700000000123\n",
            &[("job".into(), "api".into())],
            1700000000000,
            ScrapeLimits::default(),
        )
        .unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].name, "http_requests_total");
        assert_eq!(samples[0].timestamp, 1700000000123);
        assert_eq!(samples[0].help, "Requests.");
        assert!(
            samples[0]
                .labels
                .iter()
                .any(|(key, value)| key == "job" && value == "api")
        );
    }

    #[test]
    fn parses_comments_escaped_values_and_merges_target_labels() {
        let samples = parse_prometheus_text(
            r#"# a comment
# HELP request_total Requests.
# TYPE request_total counter
request_total{job="scrape",path="a\\b\"c\n",code="200"} 3.5 1700000000123
"#,
            &[
                ("job".into(), "target-job".into()),
                ("cluster".into(), "prod".into()),
            ],
            1700000000000,
            ScrapeLimits::default(),
        )
        .unwrap();

        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 3.5);
        assert_eq!(samples[0].timestamp, 1700000000123);
        assert_eq!(samples[0].help, "Requests.");
        assert_eq!(samples[0].metric_type, MetricType::Counter);
        assert_eq!(
            samples[0].labels,
            vec![
                ("job".into(), "scrape".into()),
                ("cluster".into(), "prod".into()),
                ("path".into(), "a\\b\"c\n".into()),
                ("code".into(), "200".into()),
            ]
        );
    }

    #[test]
    fn uses_default_timestamp_and_ignores_malformed_or_special_values() {
        let samples = parse_prometheus_text(
            "good 1\n".to_owned().as_str(),
            &[],
            42,
            ScrapeLimits::default(),
        )
        .unwrap();
        assert_eq!(samples[0].timestamp, 42);

        let samples = parse_prometheus_text(
            "good 1\n".to_owned().as_str(),
            &[],
            42,
            ScrapeLimits::default(),
        )
        .unwrap();
        assert_eq!(samples.len(), 1);

        let body = "good 1\n".to_string()
            + "bad-not-a-sample\n"
            + "nan NaN\n"
            + "plus_inf +Inf\n"
            + "minus_inf -Inf\n"
            + "also_good 2 100\n";
        let samples = parse_prometheus_text(&body, &[], 42, ScrapeLimits::default()).unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].name, "good");
        assert_eq!(samples[1].name, "also_good");
        assert_eq!(samples[1].timestamp, 100);
    }

    #[test]
    fn rejects_malformed_label_sets() {
        assert!(parse_labels("label=unquoted").is_err());
        assert!(parse_labels("label=\"unclosed").is_err());
        assert!(parse_labels("label=\"value\" trailing").is_err());
        assert!(parse_labels("=\"value\"").is_err());
    }

    #[test]
    fn preserves_unicode_label_values() {
        let labels = parse_labels("city=\"Montréal\",team=\"東京\"").unwrap();
        assert_eq!(labels[0].value, "Montréal");
        assert_eq!(labels[1].value, "東京");
    }

    #[test]
    fn rejects_invalid_escapes_and_duplicate_sample_labels() {
        assert!(parse_labels(r#"path="bad\tvalue""#).is_err());
        assert!(parse_labels(r#"env="dev",env="prod""#).is_err());
    }

    #[test]
    fn rejects_duplicate_target_labels() {
        let result = PrometheusTextParser::new(
            &[("env".into(), "dev".into()), ("env".into(), "prod".into())],
            42,
            ScrapeLimits::default(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn scrape_parser_enforces_sample_and_label_limits() {
        let limits = ScrapeLimits {
            max_samples_per_target: 1,
            ..ScrapeLimits::default()
        };
        assert!(parse_prometheus_text("a 1\nb 2\n", &[], 42, limits).is_err());

        let limits = ScrapeLimits {
            max_labels_per_sample: 2,
            ..ScrapeLimits::default()
        };
        assert!(
            parse_prometheus_text(
                "a{one=\"1\",two=\"2\"} 1\n",
                &[("target".into(), "value".into())],
                42,
                limits,
            )
            .is_err()
        );
    }

    #[test]
    fn scrape_parser_enforces_name_value_and_line_limits() {
        let name_limits = ScrapeLimits {
            max_metric_name_bytes: 3,
            ..ScrapeLimits::default()
        };
        assert!(parse_prometheus_text("long 1\n", &[], 42, name_limits).is_err());

        let label_name_limits = ScrapeLimits {
            max_label_name_bytes: 3,
            ..ScrapeLimits::default()
        };
        assert!(parse_prometheus_text("ok{long=\"v\"} 1\n", &[], 42, label_name_limits,).is_err());

        let label_value_limits = ScrapeLimits {
            max_label_value_bytes: 3,
            ..ScrapeLimits::default()
        };
        assert!(
            parse_prometheus_text("ok{key=\"long\"} 1\n", &[], 42, label_value_limits,).is_err()
        );

        let line_limits = ScrapeLimits {
            max_line_bytes: 4,
            ..ScrapeLimits::default()
        };
        assert!(parse_prometheus_text("metric 1\n", &[], 42, line_limits).is_err());
    }

    #[test]
    fn incremental_scrape_parser_applies_metadata_at_finish() {
        let mut parser =
            PrometheusTextParser::new(&[("job".into(), "api".into())], 42, ScrapeLimits::default())
                .unwrap();
        parser.push_line("requests_total 3").unwrap();
        parser.push_line("# HELP requests_total Requests.").unwrap();
        parser.push_line("# TYPE requests_total counter").unwrap();
        let samples = parser.finish();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].help, "Requests.");
        assert_eq!(samples[0].metric_type, MetricType::Counter);
        assert_eq!(samples[0].labels, vec![("job".into(), "api".into())]);
    }

    #[test]
    fn encodes_protobuf_with_sorted_merged_labels_and_metadata() {
        let metric = CollectedMetric {
            name: "http_requests_total".into(),
            labels: vec![("zone".into(), "west".into()), ("job".into(), "api".into())],
            value: 9.5,
            timestamp: 1700000000123,
            help: "Total requests.".into(),
            metric_type: MetricType::Counter,
        };
        let encoded = encode(None, &[metric], &BTreeMap::new(), false).unwrap();
        assert_eq!(encoded.series, 1);
        assert_eq!(encoded.samples, 1);

        let request = WriteRequest::decode(encoded.bytes.as_slice()).unwrap();
        assert_eq!(request.timeseries.len(), 1);
        assert_eq!(request.metadata.len(), 1);
        assert_eq!(
            request.metadata[0].metric_family_name,
            "http_requests_total"
        );
        assert_eq!(request.metadata[0].help, "Total requests.");
        assert_eq!(request.metadata[0].r#type, MetricType::Counter as i32);
        assert_eq!(
            request.timeseries[0]
                .labels
                .iter()
                .map(|label| label.name.as_str())
                .collect::<Vec<_>>(),
            vec!["__name__", "job", "zone"]
        );
        assert_eq!(request.timeseries[0].labels[0].value, "http_requests_total");
        assert_eq!(request.timeseries[0].samples[0].value, 9.5);
        assert_eq!(request.timeseries[0].samples[0].timestamp, 1700000000123);
    }

    #[test]
    fn encoder_keeps_only_the_canonical_metric_name_label() {
        let metric = CollectedMetric {
            name: "requests_total".into(),
            labels: vec![("__name__".into(), "spoofed_name".into())],
            value: 1.0,
            timestamp: 42,
            help: String::new(),
            metric_type: MetricType::Counter,
        };
        let encoded = encode(None, &[metric], &BTreeMap::new(), false).unwrap();
        let request = WriteRequest::decode(encoded.bytes.as_slice()).unwrap();
        let name_labels = request.timeseries[0]
            .labels
            .iter()
            .filter(|label| label.name == "__name__")
            .collect::<Vec<_>>();
        assert_eq!(name_labels.len(), 1);
        assert_eq!(name_labels[0].value, "requests_total");
    }

    #[test]
    fn encoder_emits_one_metadata_record_per_metric_family() {
        let metrics = [1.0, 2.0].map(|value| CollectedMetric {
            name: "queue_depth".into(),
            labels: vec![("worker".into(), value.to_string())],
            value,
            timestamp: 42,
            help: "Queued work.".into(),
            metric_type: MetricType::Gauge,
        });
        let encoded = encode(None, &metrics, &BTreeMap::new(), false).unwrap();
        let request = WriteRequest::decode(encoded.bytes.as_slice()).unwrap();
        assert_eq!(request.timeseries.len(), 2);
        assert_eq!(request.metadata.len(), 1);
        assert_eq!(request.metadata[0].metric_family_name, "queue_depth");
    }

    #[test]
    fn self_metrics_require_a_snapshot() {
        let error = encode(None, &[], &BTreeMap::new(), true).err().unwrap();
        assert!(error.to_string().contains("require a status snapshot"));
    }

    #[test]
    fn snappy_round_trip_preserves_remote_write_payload() {
        let metric = CollectedMetric {
            name: "queue_depth".into(),
            labels: Vec::new(),
            value: 4.0,
            timestamp: 99,
            help: String::new(),
            metric_type: MetricType::Gauge,
        };
        let encoded = encode(None, &[metric], &BTreeMap::new(), false).unwrap();
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&encoded.bytes)
            .unwrap();
        let decompressed = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        let request = WriteRequest::decode(decompressed.as_slice()).unwrap();
        assert_eq!(request.timeseries[0].labels[0].name, "__name__");
        assert_eq!(request.timeseries[0].samples[0].value, 4.0);
    }

    #[test]
    fn self_metrics_add_reserved_name_and_default_labels() {
        let snapshot = snapshot();
        let encoded = encode(Some(&snapshot), &[], &BTreeMap::new(), true).unwrap();
        let request = WriteRequest::decode(encoded.bytes.as_slice()).unwrap();
        let ready = request
            .timeseries
            .iter()
            .find(|series| {
                series
                    .labels
                    .iter()
                    .any(|label| label.name == "__name__" && label.value == "metrics_agent_ready")
            })
            .unwrap();
        assert!(
            ready
                .labels
                .iter()
                .any(|label| { label.name == "job" && label.value == "metrics-agent" })
        );
        assert!(
            ready
                .labels
                .iter()
                .any(|label| { label.name == "service" && label.value == "metrics-agent" })
        );
    }

    #[test]
    fn extra_labels_apply_to_every_series_and_override_existing_values() {
        let metric = CollectedMetric {
            name: "http_requests_total".into(),
            labels: vec![("env".into(), "prod".into()), ("job".into(), "api".into())],
            value: 1.0,
            timestamp: 1700000000123,
            help: String::new(),
            metric_type: MetricType::Counter,
        };
        let extra_labels = BTreeMap::from([
            ("cluster".to_string(), "ntt-japan".to_string()),
            ("env".to_string(), "dev".to_string()),
        ]);
        let snapshot = snapshot();
        let encoded = encode(Some(&snapshot), &[metric], &extra_labels, true).unwrap();
        let request = WriteRequest::decode(encoded.bytes.as_slice()).unwrap();

        assert!(!request.timeseries.is_empty());
        for series in request.timeseries {
            assert!(
                series
                    .labels
                    .iter()
                    .any(|label| label.name == "env" && label.value == "dev")
            );
            assert!(
                series
                    .labels
                    .iter()
                    .any(|label| label.name == "cluster" && label.value == "ntt-japan")
            );
        }
    }
}
