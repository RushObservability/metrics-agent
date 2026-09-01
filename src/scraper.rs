//! Lightweight Prometheus scraping from the Kubernetes scrape CRDs.
//!
//! This intentionally implements the common ServiceMonitor/PodMonitor and
//! static target shapes directly instead of depending on an external scrape
//! agent or VictoriaMetrics. The controller remains useful when those operators are
//! not installed, while native VM scrape objects can still be used as input.

use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, stream};
use kube::{
    Client,
    api::{Api, ApiResource, DynamicObject, ListParams, ResourceExt},
    core::GroupVersionKind,
};
use reqwest::Url;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use serde_json::Value;
use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    controller::{Controller, ResourcePair},
    remote_write::{CollectedMetric, PrometheusTextParser, ScrapeLimits, ScrapeMessage},
    status::{MetricCardinality, ScrapeStatus},
};

#[derive(Clone, Debug)]
struct TargetIdentity {
    key: String,
    pair: String,
    source: String,
    namespace: String,
    name: String,
}

#[derive(Clone, Debug)]
struct ScrapeTarget {
    url: String,
    labels: Vec<(String, String)>,
    identity: TargetIdentity,
}

#[derive(Default)]
struct DiscoveryData {
    services: Vec<Value>,
    endpoints: HashMap<(String, String), Value>,
    pods: Vec<Value>,
}

#[derive(Default)]
struct TargetCache {
    refreshed_at: Option<Instant>,
    targets: Arc<[ScrapeTarget]>,
}

impl TargetCache {
    async fn get(
        &mut self,
        client: Client,
        pairs: &[ResourcePair],
        refresh_interval: Duration,
    ) -> Result<(Arc<[ScrapeTarget]>, u64, bool)> {
        let fresh = self
            .refreshed_at
            .is_some_and(|refreshed_at| refreshed_at.elapsed() < refresh_interval);
        if fresh {
            return Ok((Arc::clone(&self.targets), 0, true));
        }

        let started = Instant::now();
        match discover_targets(client, pairs).await {
            Ok(targets) => {
                let duration_ms = started.elapsed().as_millis() as u64;
                let targets: Arc<[ScrapeTarget]> = targets.into();
                self.targets = Arc::clone(&targets);
                self.refreshed_at = Some(Instant::now());
                Ok((targets, duration_ms, false))
            }
            Err(error) => {
                let stale_allowed = self.refreshed_at.is_some_and(|refreshed_at| {
                    refreshed_at.elapsed()
                        < refresh_interval.checked_mul(2).unwrap_or(Duration::MAX)
                });
                if stale_allowed && !self.targets.is_empty() {
                    warn!(%error, "scrape target refresh failed; using bounded stale discovery data");
                    Ok((
                        Arc::clone(&self.targets),
                        started.elapsed().as_millis() as u64,
                        true,
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }
}

pub async fn run(
    controller: Arc<Controller>,
    sender: mpsc::Sender<ScrapeMessage>,
    shutdown: CancellationToken,
) {
    let http = match reqwest::Client::builder()
        .timeout(controller.config().scrape_timeout)
        .user_agent("rush-metrics-agent")
        // A redirect target has not passed the destination checks below.
        .redirect(reqwest::redirect::Policy::none())
        // Re-check every connection-time DNS answer so a name cannot pass the
        // discovery check and then rebind to a protected address.
        .dns_resolver(Arc::new(SafeResolver {
            allowed: controller.config().scrape_allowed_destinations.clone(),
        }))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            warn!(error = %error, "unable to build scrape HTTP client");
            return;
        }
    };
    let mut interval = time::interval(controller.config().scrape_interval);
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut target_cache = TargetCache::default();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {
                controller.status.begin_scrape().await;
                let started = Instant::now();
                let result = scrape_once(&controller, &http, &sender, &mut target_cache).await;
                let duration_ms = started.elapsed().as_millis() as u64;
                let timestamp = chrono::Utc::now().to_rfc3339();
                match result {
                    Ok((targets, healthy, samples, discovery_duration_ms, discovery_cache_hit)) => {
                        controller.status.set_scrape_status(ScrapeStatus {
                            enabled: true,
                            targets: targets as u64,
                            healthy_targets: healthy as u64,
                            samples,
                            errors: targets.saturating_sub(healthy) as u64,
                            duration_ms,
                            discovery_duration_ms,
                            discovery_cache_hit,
                            last_scrape_at: Some(timestamp),
                            last_error: None,
                        }).await;
                    }
                    Err(error) => {
                        let _ = sender
                            .send(ScrapeMessage::Complete {
                                targets: 0,
                                healthy_targets: 0,
                                samples: 0,
                                error: Some(error.to_string()),
                            })
                            .await;
                        controller.status.set_scrape_status(ScrapeStatus {
                            enabled: true,
                            last_scrape_at: Some(timestamp),
                            last_error: Some(error.to_string()),
                            duration_ms,
                            ..Default::default()
                        }).await;
                    }
                }
            }
        }
    }
}

async fn scrape_once(
    controller: &Controller,
    http: &reqwest::Client,
    sender: &mpsc::Sender<ScrapeMessage>,
    target_cache: &mut TargetCache,
) -> Result<(usize, usize, u64, u64, bool)> {
    let (targets, discovery_duration_ms, discovery_cache_hit) = target_cache
        .get(
            controller.kube_client(),
            controller.resource_pairs(),
            controller.config().scrape_discovery_refresh_interval,
        )
        .await?;
    let limits = ScrapeLimits {
        max_response_bytes: controller.config().scrape_max_response_bytes,
        max_samples_per_target: controller.config().scrape_max_samples_per_target,
        max_labels_per_sample: controller.config().scrape_max_labels_per_sample,
        max_label_name_bytes: controller.config().scrape_max_label_name_bytes,
        max_label_value_bytes: controller.config().scrape_max_label_value_bytes,
        max_metric_name_bytes: controller.config().scrape_max_metric_name_bytes,
        max_line_bytes: controller.config().scrape_max_line_bytes,
    };
    let total = targets.len();
    sender
        .send(ScrapeMessage::Start { targets: total })
        .await
        .context("metrics remote-write channel closed")?;
    let mut results = stream::iter((0..total).map(|index| {
        let targets = Arc::clone(&targets);
        async move {
            let target = &targets[index];
            let identity = &target.identity;
            if !namespace_allowed(
                &identity.namespace,
                &controller.config().scrape_allowed_namespaces,
            ) {
                bail!("scrape source namespace is not allowed");
            }
            let url = validate_scrape_url(
                &target.url,
                &controller.config().scrape_allowed_destinations,
            )?;
            let response = http
                .get(url)
                .send()
                .await
                .map_err(|_| anyhow::anyhow!("scrape request failed"))?;
            if response.status().is_redirection() {
                bail!("scrape redirects are disabled");
            }
            if !response.status().is_success() {
                bail!("scrape returned HTTP {}", response.status());
            }
            let metrics = parse_bounded_response(
                response,
                &target.labels,
                chrono::Utc::now().timestamp_millis(),
                limits,
            )
            .await?;
            Ok::<_, anyhow::Error>((index, metrics))
        }
    }))
    .buffer_unordered(controller.config().scrape_concurrency);
    let mut healthy = 0;
    let mut samples = 0;
    while let Some(result) = results.next().await {
        match result {
            Ok((index, metrics)) => {
                let identity = &targets[index].identity;
                healthy += 1;
                let target_samples = metrics.len() as u64;
                let mut counts = HashMap::new();
                for metric in &metrics {
                    *counts.entry(metric.name.clone()).or_insert(0) += 1;
                }
                controller
                    .status
                    .record_crd_metrics(
                        identity.key.clone(),
                        identity.pair.clone(),
                        identity.source.clone(),
                        identity.namespace.clone(),
                        identity.name.clone(),
                        target_samples,
                        counts
                            .into_iter()
                            .map(|(name, series)| MetricCardinality { name, series }),
                    )
                    .await;
                samples += send_metric_chunks(sender, metrics).await?;
            }
            Err(error) => debug!(reason = target_error_key(&error), "scrape target failed"),
        }
    }
    sender
        .send(ScrapeMessage::Complete {
            targets: total,
            healthy_targets: healthy,
            samples,
            error: None,
        })
        .await
        .context("metrics remote-write channel closed")?;
    Ok((
        total,
        healthy,
        samples,
        discovery_duration_ms,
        discovery_cache_hit,
    ))
}

fn target_error_key(error: &anyhow::Error) -> &'static str {
    let message = error.to_string();
    if message.contains("namespace") {
        "namespace_denied"
    } else if message.contains("destination") || message.contains("URL") {
        "destination_denied"
    } else if message.contains("redirect") {
        "redirect_denied"
    } else if message.contains("too large") || message.contains("limit") {
        "resource_limit"
    } else {
        "request_failed"
    }
}

fn namespace_allowed(namespace: &str, allowed: &[String]) -> bool {
    allowed.is_empty()
        || allowed
            .iter()
            .any(|candidate| candidate.trim() == namespace)
}

fn destination_explicitly_allowed(host: &str, allowed: &[String]) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    allowed.iter().any(|candidate| {
        let candidate = candidate.trim().trim_end_matches('.').to_ascii_lowercase();
        if let Some(suffix) = candidate.strip_prefix("*.") {
            host != suffix && host.ends_with(&format!(".{suffix}"))
        } else {
            !candidate.is_empty() && host == candidate
        }
    })
}

fn kubernetes_api_host(host: &str) -> bool {
    let normalized = host.trim_end_matches('.').to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        "kubernetes" | "kubernetes.default" | "kubernetes.default.svc"
    ) || normalized.starts_with("kubernetes.default.svc.")
    {
        return true;
    }
    std::env::var("KUBERNETES_SERVICE_HOST")
        .ok()
        .is_some_and(|configured| configured.trim_matches(['[', ']']) == normalized)
}

fn kubernetes_service_ip() -> Option<IpAddr> {
    std::env::var("KUBERNETES_SERVICE_HOST")
        .ok()
        .and_then(|configured| configured.trim_matches(['[', ']']).parse().ok())
}

fn unsafe_destination_ip(ip: IpAddr) -> bool {
    if kubernetes_service_ip() == Some(ip) {
        return true;
    }
    match ip {
        IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip == Ipv4Addr::new(169, 254, 169, 254)
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || (ip.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

#[derive(Clone, Debug)]
struct SafeResolver {
    allowed: Vec<String>,
}

impl Resolve for SafeResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        let allowed = destination_explicitly_allowed(&host, &self.allowed);
        Box::pin(async move {
            if !allowed && kubernetes_api_host(&host) {
                return Err(resolution_denied());
            }
            let addresses = tokio::net::lookup_host((host.as_str(), 0))
                .await
                .map_err(|error| {
                    Box::new(error) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?
                .collect::<Vec<_>>();
            if addresses.is_empty()
                || (!allowed
                    && addresses
                        .iter()
                        .any(|address| unsafe_destination_ip(address.ip())))
            {
                return Err(resolution_denied());
            }
            Ok(Box::new(addresses.into_iter()) as Addrs)
        })
    }
}

fn resolution_denied() -> Box<dyn std::error::Error + Send + Sync + 'static> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "scrape destination is blocked",
    ))
}

fn validate_scrape_url(raw: &str, allowed: &[String]) -> Result<Url> {
    let url = Url::parse(raw).context("scrape target is not a valid URL")?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("scrape target URL scheme is not allowed");
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        bail!("scrape target URL contains forbidden credentials or fragment");
    }
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("scrape target URL has no host"))?;
    if destination_explicitly_allowed(host, allowed) {
        return Ok(url);
    }
    if kubernetes_api_host(host) {
        bail!("scrape destination is blocked");
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if unsafe_destination_ip(ip) {
            bail!("scrape destination is blocked");
        }
        return Ok(url);
    }

    // Hostname resolution and IP-range enforcement happen in SafeResolver at
    // connection time. A separate preflight lookup doubles DNS work and still
    // cannot prevent rebinding between validation and connection.
    Ok(url)
}

async fn parse_bounded_response(
    response: reqwest::Response,
    target_labels: &[(String, String)],
    default_timestamp: i64,
    limits: ScrapeLimits,
) -> Result<Vec<CollectedMetric>> {
    if response
        .content_length()
        .is_some_and(|length| length > limits.max_response_bytes as u64)
    {
        bail!("scrape response exceeds the configured byte limit");
    }
    let mut parser = BoundedTextParser::new(target_labels, default_timestamp, limits)?;
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.context("reading scrape response body")?;
        parser.push_chunk(&chunk)?;
    }
    parser.finish()
}

/// Keeps response framing and parser limits testable without opening a socket.
struct BoundedTextParser {
    parser: PrometheusTextParser,
    pending: Vec<u8>,
    total_bytes: usize,
    limits: ScrapeLimits,
}

impl BoundedTextParser {
    fn new(
        target_labels: &[(String, String)],
        default_timestamp: i64,
        limits: ScrapeLimits,
    ) -> Result<Self> {
        Ok(Self {
            parser: PrometheusTextParser::new(target_labels, default_timestamp, limits)?,
            pending: Vec::with_capacity(limits.max_line_bytes.min(8_192)),
            total_bytes: 0,
            limits,
        })
    }

    fn push_chunk(&mut self, chunk: &[u8]) -> Result<()> {
        account_response_bytes(
            &mut self.total_bytes,
            chunk.len(),
            self.limits.max_response_bytes,
        )?;
        self.pending.extend_from_slice(chunk);

        let mut consumed = 0usize;
        for newline in self
            .pending
            .iter()
            .enumerate()
            .filter_map(|(index, byte)| (*byte == b'\n').then_some(index))
        {
            let mut line = &self.pending[consumed..newline];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            self.parser.push_line(
                std::str::from_utf8(line).context("scrape response is not valid UTF-8")?,
            )?;
            consumed = newline + 1;
        }
        if consumed > 0 {
            self.pending.drain(..consumed);
        }
        if self.pending.len() > self.limits.max_line_bytes {
            bail!("Prometheus exposition line exceeds the configured byte limit");
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<CollectedMetric>> {
        if !self.pending.is_empty() {
            self.parser.push_line(
                std::str::from_utf8(&self.pending).context("scrape response is not valid UTF-8")?,
            )?;
        }
        Ok(self.parser.finish())
    }
}

fn account_response_bytes(total: &mut usize, chunk_bytes: usize, max_bytes: usize) -> Result<()> {
    if chunk_bytes > max_bytes.saturating_sub(*total) {
        bail!("scrape response exceeds the configured byte limit");
    }
    *total += chunk_bytes;
    Ok(())
}

async fn send_metric_chunks(
    sender: &mpsc::Sender<ScrapeMessage>,
    metrics: Vec<CollectedMetric>,
) -> Result<u64> {
    let samples = metrics.len() as u64;
    let mut chunk = Vec::with_capacity(10_000);
    for metric in metrics {
        chunk.push(metric);
        if chunk.len() == 10_000 {
            sender
                .send(ScrapeMessage::Batch(std::mem::replace(
                    &mut chunk,
                    Vec::with_capacity(10_000),
                )))
                .await
                .context("metrics remote-write channel closed")?;
        }
    }
    if !chunk.is_empty() {
        sender
            .send(ScrapeMessage::Batch(chunk))
            .await
            .context("metrics remote-write channel closed")?;
    }
    Ok(samples)
}

async fn discover_targets(client: Client, pairs: &[ResourcePair]) -> Result<Vec<ScrapeTarget>> {
    let data = load_discovery_data(&client).await?;
    let mut targets = Vec::new();
    for pair in pairs {
        let vm_api = Api::<DynamicObject>::all_with(client.clone(), &pair.victoria);
        let vm_objects = vm_api
            .list(&ListParams::default())
            .await
            .map(|list| list.items)
            .unwrap_or_default();
        let mut selected = Vec::new();
        let mut keys = HashSet::new();
        for object in vm_objects {
            keys.insert(object_key(&object));
            selected.push((object, "victoria"));
        }
        let prom_api = Api::<DynamicObject>::all_with(client.clone(), &pair.prometheus);
        if let Ok(list) = prom_api.list(&ListParams::default()).await {
            for object in list.items {
                if keys.insert(object_key(&object)) {
                    selected.push((object, "prometheus"));
                }
            }
        }
        for (object, source) in selected {
            targets.extend(targets_for_object(pair.name, source, &object, &data));
        }
    }
    Ok(deduplicate_targets(targets))
}

async fn load_discovery_data(client: &Client) -> Result<DiscoveryData> {
    let services = list_core(client, "Service", "services").await?;
    let endpoints = list_core(client, "Endpoints", "endpoints")
        .await?
        .into_iter()
        .filter_map(|object| {
            let value = serde_json::to_value(&object).ok()?;
            Some(((namespace(&value), name(&value)), value))
        })
        .collect();
    let pods = list_core(client, "Pod", "pods").await?;
    Ok(DiscoveryData {
        services: services
            .into_iter()
            .filter_map(|object| serde_json::to_value(object).ok())
            .collect(),
        endpoints,
        pods: pods
            .into_iter()
            .filter_map(|object| serde_json::to_value(object).ok())
            .collect(),
    })
}

async fn list_core(client: &Client, kind: &str, plural: &str) -> Result<Vec<DynamicObject>> {
    let mut resource = ApiResource::from_gvk(&GroupVersionKind::gvk("", "v1", kind));
    resource.plural = plural.to_string();
    Api::<DynamicObject>::all_with(client.clone(), &resource)
        .list(&ListParams::default())
        .await
        .with_context(|| format!("list Kubernetes {kind} objects"))
        .map(|list| list.items)
}

fn targets_for_object(
    pair: &str,
    source: &str,
    object: &DynamicObject,
    data: &DiscoveryData,
) -> Vec<ScrapeTarget> {
    let value = match serde_json::to_value(object) {
        Ok(value) => value,
        Err(_) => return Vec::new(),
    };
    let spec = value.get("spec").unwrap_or(&Value::Null);
    let object_name = name(&value);
    let object_namespace = namespace(&value);
    let identity = TargetIdentity {
        key: format!("{source}:{pair}:{object_namespace}/{object_name}"),
        pair: pair.to_string(),
        source: source.to_string(),
        namespace: object_namespace.clone(),
        name: object_name,
    };
    match pair {
        "service-monitor" => service_targets(spec, &identity, data),
        "pod-monitor" => pod_targets(spec, &identity, data),
        "probe" => probe_targets(spec, &identity),
        "scrape-config" => static_targets(spec, &identity),
        _ => Vec::new(),
    }
}

fn service_targets(
    spec: &Value,
    identity: &TargetIdentity,
    data: &DiscoveryData,
) -> Vec<ScrapeTarget> {
    let selector = spec.get("selector").unwrap_or(&Value::Null);
    let namespaces = selected_namespaces(spec.get("namespaceSelector"), &identity.namespace);
    let endpoints = spec
        .get("endpoints")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| vec![Value::Object(Default::default())]);
    let mut targets = Vec::new();
    for service in &data.services {
        let service_namespace = namespace(service);
        if !namespaces
            .iter()
            .any(|value| value == "*" || value == &service_namespace)
            || !selector_matches(
                service.get("metadata").and_then(|v| v.get("labels")),
                selector,
            )
        {
            continue;
        }
        let service_name = name(service);
        let endpoint_data = data
            .endpoints
            .get(&(service_namespace.clone(), service_name.clone()));
        for endpoint in &endpoints {
            let path = endpoint
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("/metrics");
            let scheme = endpoint
                .get("scheme")
                .and_then(Value::as_str)
                .unwrap_or("http");
            let requested_port = endpoint.get("port").or_else(|| endpoint.get("targetPort"));
            for (ip, port) in endpoint_addresses(endpoint_data, requested_port, service) {
                let instance = format!("{ip}:{port}");
                targets.push(ScrapeTarget {
                    url: build_url(scheme, &instance, path),
                    labels: vec![
                        ("job".into(), identity.name.clone()),
                        ("namespace".into(), service_namespace.clone()),
                        ("service".into(), service_name.clone()),
                        ("instance".into(), instance),
                    ],
                    identity: identity.clone(),
                });
            }
        }
    }
    targets
}

fn pod_targets(spec: &Value, identity: &TargetIdentity, data: &DiscoveryData) -> Vec<ScrapeTarget> {
    let selector = spec.get("selector").unwrap_or(&Value::Null);
    let namespaces = selected_namespaces(spec.get("namespaceSelector"), &identity.namespace);
    let endpoints = spec
        .get("podMetricsEndpoints")
        .or_else(|| spec.get("endpoints"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_else(|| vec![Value::Object(Default::default())]);
    let mut targets = Vec::new();
    for pod in &data.pods {
        let pod_namespace = namespace(pod);
        if !namespaces
            .iter()
            .any(|value| value == "*" || value == &pod_namespace)
            || !selector_matches(pod.get("metadata").and_then(|v| v.get("labels")), selector)
        {
            continue;
        }
        let Some(ip) = pod
            .get("status")
            .and_then(|v| v.get("podIP"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        for endpoint in &endpoints {
            let requested_port = endpoint.get("port").or_else(|| endpoint.get("targetPort"));
            let port = pod_port(pod, requested_port).unwrap_or(80);
            let path = endpoint
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("/metrics");
            let scheme = endpoint
                .get("scheme")
                .and_then(Value::as_str)
                .unwrap_or("http");
            let instance = format!("{ip}:{port}");
            targets.push(ScrapeTarget {
                url: build_url(scheme, &instance, path),
                labels: vec![
                    ("job".into(), identity.name.clone()),
                    ("namespace".into(), pod_namespace.clone()),
                    ("pod".into(), name(pod)),
                    ("instance".into(), instance),
                ],
                identity: identity.clone(),
            });
        }
    }
    targets
}

fn static_targets(spec: &Value, identity: &TargetIdentity) -> Vec<ScrapeTarget> {
    let configs = spec
        .get("staticConfigs")
        .or_else(|| spec.get("static_configs"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let scheme = spec.get("scheme").and_then(Value::as_str).unwrap_or("http");
    let path = spec
        .get("metricsPath")
        .or_else(|| spec.get("path"))
        .and_then(Value::as_str)
        .unwrap_or("/metrics");
    configs
        .into_iter()
        .flat_map(|config| {
            let labels = config
                .get("labels")
                .and_then(Value::as_object)
                .map(|labels| {
                    labels
                        .iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            config
                .get("targets")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(move |target| {
                    let target = target.as_str()?;
                    let instance = target.to_string();
                    let mut target_labels = labels.clone();
                    if !target_labels.iter().any(|(key, _)| key == "job") {
                        target_labels.push(("job".into(), identity.name.clone()));
                    }
                    if !target_labels.iter().any(|(key, _)| key == "instance") {
                        target_labels.push(("instance".into(), instance.clone()));
                    }
                    Some(ScrapeTarget {
                        url: normalize_target(target, scheme, path),
                        labels: target_labels,
                        identity: identity.clone(),
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn probe_targets(spec: &Value, identity: &TargetIdentity) -> Vec<ScrapeTarget> {
    let prober = spec
        .get("prober")
        .and_then(|v| v.get("url"))
        .and_then(Value::as_str);
    let Some(prober) = prober else {
        return Vec::new();
    };
    let targets = spec
        .get("targets")
        .and_then(|v| v.get("staticConfig"))
        .and_then(|v| v.get("static"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    targets
        .into_iter()
        .filter_map(|value| {
            let target = value.as_str()?;
            let mut url = Url::parse(prober).ok()?;
            url.query_pairs_mut().append_pair("target", target);
            Some(ScrapeTarget {
                url: url.to_string(),
                labels: vec![
                    ("job".into(), identity.name.clone()),
                    ("instance".into(), target.into()),
                ],
                identity: identity.clone(),
            })
        })
        .collect()
}

fn endpoint_addresses(
    endpoint_data: Option<&Value>,
    requested: Option<&Value>,
    service: &Value,
) -> Vec<(String, u16)> {
    let Some(data) = endpoint_data else {
        return Vec::new();
    };
    let requested = requested.and_then(value_string);
    let mut result = Vec::new();
    for subset in data
        .get("subsets")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let port = subset
            .get("ports")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find_map(|port| {
                let name = port.get("name").and_then(Value::as_str);
                let number = port.get("port").and_then(Value::as_u64).map(|v| v as u16);
                match requested.as_deref() {
                    Some(requested)
                        if name == Some(requested)
                            || number.map(|v| v.to_string()) == Some(requested.to_string()) =>
                    {
                        number
                    }
                    Some(_) => None,
                    None => number,
                }
            });
        let Some(port) = port.or_else(|| service_port(service, requested.as_deref())) else {
            continue;
        };
        for address in subset
            .get("addresses")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(ip) = address.get("ip").and_then(Value::as_str) {
                result.push((ip.to_string(), port));
            }
        }
    }
    result
}

fn service_port(service: &Value, requested: Option<&str>) -> Option<u16> {
    service
        .get("spec")
        .and_then(|v| v.get("ports"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find_map(|port| {
            let name = port.get("name").and_then(Value::as_str);
            if requested.is_none()
                || requested == name
                || requested
                    == port
                        .get("port")
                        .and_then(Value::as_u64)
                        .map(|v| v.to_string())
                        .as_deref()
            {
                port.get("port").and_then(Value::as_u64).map(|v| v as u16)
            } else {
                None
            }
        })
}

fn pod_port(pod: &Value, requested: Option<&Value>) -> Option<u16> {
    let requested = requested.and_then(value_string);
    pod.get("spec")
        .and_then(|v| v.get("containers"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(|container| {
            container
                .get("ports")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .find_map(|port| {
            let name = port.get("name").and_then(Value::as_str);
            let number = port
                .get("containerPort")
                .and_then(Value::as_u64)
                .map(|v| v as u16);
            if requested.is_none()
                || requested.as_deref() == name
                || requested.as_deref() == number.map(|v| v.to_string()).as_deref()
            {
                number
            } else {
                None
            }
        })
}

fn selector_matches(labels: Option<&Value>, selector: &Value) -> bool {
    selector
        .get("matchLabels")
        .and_then(Value::as_object)
        .map(|required| {
            required.iter().all(|(key, value)| {
                labels
                    .and_then(Value::as_object)
                    .and_then(|labels| labels.get(key))
                    == Some(value)
            })
        })
        .unwrap_or(true)
}

fn selected_namespaces(selector: Option<&Value>, own: &str) -> Vec<String> {
    let Some(selector) = selector else {
        return vec![own.into()];
    };
    if selector.get("any").and_then(Value::as_bool) == Some(true) {
        return vec!["*".into()];
    }
    selector
        .get("matchNames")
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .filter(|names: &Vec<String>| !names.is_empty())
        .unwrap_or_else(|| vec![own.into()])
}

fn build_url(scheme: &str, host: &str, path: &str) -> String {
    format!(
        "{scheme}://{host}{}",
        if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        }
    )
}
fn normalize_target(target: &str, scheme: &str, path: &str) -> String {
    if target.starts_with("http://") || target.starts_with("https://") {
        target.to_string()
    } else {
        build_url(scheme, target, path)
    }
}
fn value_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_string)
        .or_else(|| value.as_u64().map(|value| value.to_string()))
}
fn name(value: &Value) -> String {
    value
        .get("metadata")
        .and_then(|v| v.get("name"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .into()
}
fn namespace(value: &Value) -> String {
    value
        .get("metadata")
        .and_then(|v| v.get("namespace"))
        .and_then(Value::as_str)
        .unwrap_or("default")
        .into()
}
fn object_key(object: &DynamicObject) -> String {
    format!(
        "{}/{}",
        object.namespace().unwrap_or_default(),
        object.name_any()
    )
}
fn deduplicate_targets(targets: Vec<ScrapeTarget>) -> Vec<ScrapeTarget> {
    let mut seen = HashSet::new();
    targets
        .into_iter()
        .filter(|target| seen.insert(target.url.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use serde_json::json;
    use tokio::sync::mpsc;

    use super::{
        BoundedTextParser, DiscoveryData, ScrapeTarget, TargetIdentity, account_response_bytes,
        deduplicate_targets, destination_explicitly_allowed, namespace_allowed, pod_targets,
        probe_targets, send_metric_chunks, service_targets, static_targets, targets_for_object,
        unsafe_destination_ip, validate_scrape_url,
    };
    use crate::remote_write::{CollectedMetric, MetricType, ScrapeLimits};
    use crate::status::{MetricCardinality, StatusStore};

    fn object(value: serde_json::Value) -> kube::api::DynamicObject {
        serde_json::from_value(value).unwrap()
    }

    fn identity(pair: &str, source: &str) -> TargetIdentity {
        TargetIdentity {
            key: format!("{source}:{pair}:monitoring/monitor"),
            pair: pair.into(),
            source: source.into(),
            namespace: "monitoring".into(),
            name: "monitor".into(),
        }
    }

    #[test]
    fn destination_policy_blocks_sensitive_network_ranges() {
        for ip in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            "fe80::1".parse().unwrap(),
        ] {
            assert!(unsafe_destination_ip(ip), "{ip} must be blocked");
        }
        assert!(!unsafe_destination_ip("10.0.0.20".parse().unwrap()));
    }

    #[test]
    fn explicit_destination_and_namespace_allowlists_are_exact() {
        let destinations = vec!["metadata.test".into(), "*.trusted.example".into()];
        assert!(destination_explicitly_allowed(
            "metadata.test",
            &destinations
        ));
        assert!(destination_explicitly_allowed(
            "app.trusted.example",
            &destinations
        ));
        assert!(!destination_explicitly_allowed(
            "trusted.example",
            &destinations
        ));
        assert!(!destination_explicitly_allowed(
            "eviltrusted.example",
            &destinations
        ));
        assert!(namespace_allowed("monitoring", &[]));
        assert!(namespace_allowed("monitoring", &["monitoring".into()]));
        assert!(!namespace_allowed("default", &["monitoring".into()]));
    }

    #[test]
    fn scrape_url_validation_rejects_credentials_fragments_and_local_targets() {
        assert!(validate_scrape_url("file:///etc/passwd", &[]).is_err());
        assert!(validate_scrape_url("http://user:secret@example.com/metrics", &[]).is_err());
        assert!(validate_scrape_url("http://example.com/metrics#secret", &[]).is_err());
        assert!(validate_scrape_url("http://127.0.0.1:9090/metrics", &[]).is_err());
        assert!(
            validate_scrape_url("http://127.0.0.1:9090/metrics", &["127.0.0.1".into()]).is_ok()
        );
    }

    fn discovery_data() -> DiscoveryData {
        DiscoveryData {
            services: vec![json!({
                "metadata": {
                    "name": "api",
                    "namespace": "default",
                    "labels": {"app": "api"}
                },
                "spec": {"ports": [{"name": "metrics", "port": 9100}]}
            })],
            endpoints: [(
                ("default".into(), "api".into()),
                json!({
                    "subsets": [{
                        "addresses": [{"ip": "10.0.0.1"}],
                        "ports": [{"name": "metrics", "port": 9100}]
                    }]
                }),
            )]
            .into_iter()
            .collect(),
            pods: vec![json!({
                "metadata": {
                    "name": "api-pod",
                    "namespace": "default",
                    "labels": {"app": "api"}
                },
                "status": {"podIP": "10.0.0.2"},
                "spec": {"containers": [{"ports": [{"name": "metrics", "containerPort": 8080}]}]}
            })],
        }
    }

    #[test]
    fn builds_service_monitor_targets_with_selector_namespace_and_labels() {
        let spec = json!({
            "selector": {"matchLabels": {"app": "api"}},
            "namespaceSelector": {"matchNames": ["default"]},
            "endpoints": [{"port": "metrics", "scheme": "https", "path": "custom"}]
        });
        let targets = service_targets(
            &spec,
            &identity("service-monitor", "victoria"),
            &discovery_data(),
        );
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].url, "https://10.0.0.1:9100/custom");
        assert_eq!(
            targets[0].identity.key,
            "victoria:service-monitor:monitoring/monitor"
        );
        assert!(
            targets[0]
                .labels
                .contains(&("job".into(), "monitor".into()))
        );
        assert!(
            targets[0]
                .labels
                .contains(&("service".into(), "api".into()))
        );
        assert!(
            targets[0]
                .labels
                .contains(&("instance".into(), "10.0.0.1:9100".into()))
        );
    }

    #[test]
    fn builds_pod_monitor_targets_from_named_container_ports() {
        let spec = json!({
            "selector": {"matchLabels": {"app": "api"}},
            "namespaceSelector": {"any": true},
            "podMetricsEndpoints": [{"port": "metrics", "scheme": "https", "path": "/pod-metrics"}]
        });
        let targets = pod_targets(
            &spec,
            &identity("pod-monitor", "prometheus"),
            &discovery_data(),
        );
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].url, "https://10.0.0.2:8080/pod-metrics");
        assert!(
            targets[0]
                .labels
                .contains(&("pod".into(), "api-pod".into()))
        );
        assert!(
            targets[0]
                .labels
                .contains(&("namespace".into(), "default".into()))
        );
    }

    #[test]
    fn builds_probe_targets_with_encoded_query_parameters() {
        let spec = json!({
            "prober": {"url": "http://blackbox:9115/probe?module=http_2xx"},
            "targets": {"staticConfig": {"static": ["https://example.com/a?x=1", "http://internal"]}}
        });
        let targets = probe_targets(&spec, &identity("probe", "victoria"));
        assert_eq!(targets.len(), 2);
        assert_eq!(
            targets[0].url,
            "http://blackbox:9115/probe?module=http_2xx&target=https%3A%2F%2Fexample.com%2Fa%3Fx%3D1"
        );
        assert!(
            targets[0]
                .labels
                .contains(&("instance".into(), "https://example.com/a?x=1".into()))
        );
    }

    #[test]
    fn builds_scrape_config_targets_and_preserves_explicit_urls() {
        let spec = json!({
            "scheme": "https",
            "metricsPath": "/custom",
            "staticConfigs": [{
                "targets": ["node-a:9100", "http://node-b:9200"],
                "labels": {"cluster": "prod"}
            }]
        });
        let targets = static_targets(&spec, &identity("scrape-config", "victoria"));
        assert_eq!(targets.len(), 2);
        assert_eq!(targets[0].url, "https://node-a:9100/custom");
        assert_eq!(targets[1].url, "http://node-b:9200");
        assert!(
            targets[0]
                .labels
                .contains(&("cluster".into(), "prod".into()))
        );
        assert!(
            targets[0]
                .labels
                .contains(&("job".into(), "monitor".into()))
        );
        assert!(
            targets[0]
                .labels
                .contains(&("instance".into(), "node-a:9100".into()))
        );
    }

    #[test]
    fn targets_for_object_assigns_crd_identity() {
        let object = object(json!({
            "apiVersion": "monitoring.coreos.com/v1",
            "kind": "ServiceMonitor",
            "metadata": {"name": "api-monitor", "namespace": "monitoring"},
            "spec": {"selector": {"matchLabels": {"app": "api"}}, "namespaceSelector": {"matchNames": ["default"]}}
        }));
        let targets =
            targets_for_object("service-monitor", "prometheus", &object, &discovery_data());
        assert_eq!(
            targets[0].identity.key,
            "prometheus:service-monitor:monitoring/api-monitor"
        );
    }

    #[test]
    fn deduplicates_by_scrape_url_and_keeps_first_target() {
        let first = ScrapeTarget {
            url: "http://10.0.0.1:9100/metrics".into(),
            labels: vec![("job".into(), "first".into())],
            identity: identity("service-monitor", "victoria"),
        };
        let second = ScrapeTarget {
            url: first.url.clone(),
            labels: vec![("job".into(), "second".into())],
            identity: identity("service-monitor", "prometheus"),
        };
        let result = deduplicate_targets(vec![first, second]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].labels[0].1, "first");
    }

    #[test]
    fn response_byte_accounting_never_exceeds_the_configured_limit() {
        let mut total = 0;
        account_response_bytes(&mut total, 4, 8).unwrap();
        account_response_bytes(&mut total, 4, 8).unwrap();
        assert_eq!(total, 8);
        assert!(account_response_bytes(&mut total, 1, 8).is_err());
        assert_eq!(total, 8);
    }

    #[test]
    fn streaming_parser_handles_split_utf8_crlf_and_final_line() {
        let body = "# TYPE temperature gauge\r\ntemperature{city=\"Montréal\"} 21\r\npressure 1013";
        let split = body.find('é').unwrap() + 1;
        let mut parser = BoundedTextParser::new(&[], 42, ScrapeLimits::default()).unwrap();
        parser.push_chunk(&body.as_bytes()[..split]).unwrap();
        parser
            .push_chunk(&body.as_bytes()[split..split + 1])
            .unwrap();
        parser.push_chunk(&body.as_bytes()[split + 1..]).unwrap();

        let metrics = parser.finish().unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].metric_type, MetricType::Gauge);
        assert_eq!(metrics[0].labels[0].1, "Montréal");
        assert_eq!(metrics[1].name, "pressure");
        assert_eq!(metrics[1].timestamp, 42);
    }

    #[test]
    fn streaming_parser_enforces_cumulative_body_limit() {
        let limits = ScrapeLimits {
            max_response_bytes: 8,
            ..ScrapeLimits::default()
        };
        let mut parser = BoundedTextParser::new(&[], 42, limits).unwrap();
        parser.push_chunk(b"a 1\n").unwrap();
        parser.push_chunk(b"b 2\n").unwrap();
        let error = parser.push_chunk(b"x").unwrap_err();
        assert!(error.to_string().contains("response exceeds"));
    }

    #[test]
    fn streaming_parser_rejects_overlong_unterminated_line() {
        let limits = ScrapeLimits {
            max_line_bytes: 4,
            ..ScrapeLimits::default()
        };
        let mut parser = BoundedTextParser::new(&[], 42, limits).unwrap();
        let error = parser.push_chunk(b"abcde").unwrap_err();
        assert!(error.to_string().contains("line exceeds"));
    }

    #[test]
    fn streaming_parser_rejects_invalid_utf8() {
        let mut parser = BoundedTextParser::new(&[], 42, ScrapeLimits::default()).unwrap();
        let error = parser
            .push_chunk(&[b'a', b' ', b'1', 0xff, b'\n'])
            .unwrap_err();
        assert!(error.to_string().contains("not valid UTF-8"));
    }

    #[test]
    fn streaming_parser_propagates_sample_limit_errors() {
        let limits = ScrapeLimits {
            max_samples_per_target: 1,
            ..ScrapeLimits::default()
        };
        let mut parser = BoundedTextParser::new(&[], 42, limits).unwrap();
        let error = parser.push_chunk(b"a 1\nb 2\n").unwrap_err();
        assert!(error.to_string().contains("sample limit"));
    }

    #[tokio::test]
    async fn sends_metrics_in_bounded_batches() {
        let metrics = (0..20_001)
            .map(|index| CollectedMetric {
                name: format!("metric_{index}"),
                labels: Vec::new(),
                value: index as f64,
                timestamp: index,
                help: String::new(),
                metric_type: MetricType::Gauge,
            })
            .collect();
        let (sender, mut receiver) = mpsc::channel(1);
        let sender_for_task = sender.clone();
        let task = tokio::spawn(async move { send_metric_chunks(&sender_for_task, metrics).await });
        let mut sizes = Vec::new();
        for _ in 0..3 {
            match receiver.recv().await.unwrap() {
                crate::remote_write::ScrapeMessage::Batch(metrics) => sizes.push(metrics.len()),
                _ => panic!("unexpected scrape message"),
            }
        }
        assert_eq!(task.await.unwrap().unwrap(), 20_001);
        assert_eq!(sizes, vec![10_000, 10_000, 1]);
    }

    #[tokio::test]
    async fn empty_metric_batch_sends_nothing() {
        let (sender, mut receiver) = mpsc::channel(1);
        assert_eq!(send_metric_chunks(&sender, Vec::new()).await.unwrap(), 0);
        drop(sender);
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn closed_remote_write_channel_stops_batching() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let metric = CollectedMetric {
            name: "up".into(),
            labels: Vec::new(),
            value: 1.0,
            timestamp: 42,
            help: String::new(),
            metric_type: MetricType::Gauge,
        };
        let error = send_metric_chunks(&sender, vec![metric]).await.unwrap_err();
        assert!(error.to_string().contains("channel closed"));
    }

    #[tokio::test]
    async fn cardinality_accumulates_per_crd_and_resets_at_scrape_start() {
        let store = StatusStore::new("test", false, "127.0.0.1:7070", "/ui/", None);
        store.begin_scrape().await;
        store
            .record_crd_metrics(
                "victoria:service-monitor:default/api".into(),
                "service-monitor".into(),
                "victoria".into(),
                "default".into(),
                "api".into(),
                5,
                [
                    MetricCardinality {
                        name: "z_metric".into(),
                        series: 2,
                    },
                    MetricCardinality {
                        name: "a_metric".into(),
                        series: 3,
                    },
                ],
            )
            .await;
        store
            .record_crd_metrics(
                "victoria:service-monitor:default/api".into(),
                "service-monitor".into(),
                "victoria".into(),
                "default".into(),
                "api".into(),
                4,
                [MetricCardinality {
                    name: "a_metric".into(),
                    series: 4,
                }],
            )
            .await;
        let snapshot = store.snapshot().await;
        let detail = &snapshot.crd_metric_cardinality[0];
        assert_eq!(detail.samples, 9);
        assert_eq!(detail.total_series, 9);
        assert_eq!(detail.top_metrics[0].name, "a_metric");
        assert_eq!(detail.top_metrics[0].series, 7);

        store.begin_scrape().await;
        assert!(store.snapshot().await.crd_metric_cardinality.is_empty());
    }
}
