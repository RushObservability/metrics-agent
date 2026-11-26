//! Lightweight Prometheus scraping from the Kubernetes scrape CRDs.
//!
//! This intentionally implements the common ServiceMonitor/PodMonitor and
//! static target shapes directly instead of depending on an external scrape
//! agent or VictoriaMetrics. The controller remains useful when those operators are
//! not installed, while native VM scrape objects can still be used as input.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, stream};
use jiff::{Timestamp, tz::Offset};
use kube::Client;
use reqwest::Url;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use serde_json::Value;
use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    controller::{Controller, ResourcePair, ScrapeInventory, ScrapeObject},
    remote_write::{CollectedMetric, PrometheusTextParser, ScrapeLimits, ScrapeMessage},
    status::{MetricCardinality, ScrapeStatus},
};

mod discovery_watch;
mod internal_target;
mod selector;

/// Reject incomplete internal scrape configuration before starting workers.
pub fn validate_internal_metrics(config: &crate::config::Config) -> Result<()> {
    internal_target::target(config).map(|_| ())
}

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
    services: BTreeMap<String, Vec<Value>>,
    endpoints: HashMap<(String, String), Value>,
    pods: BTreeMap<String, Vec<Value>>,
    allowed_target_namespaces: Vec<String>,
    allowed_source_namespaces: Vec<String>,
}

#[derive(Default)]
struct TargetCache {
    refreshed_at: Option<Instant>,
    targets: Arc<[ScrapeTarget]>,
    watches: discovery_watch::DiscoveryWatches,
}

impl TargetCache {
    async fn get(
        &mut self,
        client: Client,
        pairs: &[ResourcePair],
        inventory: &ScrapeInventory,
        refresh_interval: Duration,
        max_targets: usize,
        allowed_target_namespaces: &[String],
    ) -> Result<(Arc<[ScrapeTarget]>, u64, bool)> {
        let fresh = self
            .refreshed_at
            .is_some_and(|refreshed_at| refreshed_at.elapsed() < refresh_interval);
        if fresh {
            crate::discovery_metrics::cache(
                "fresh",
                self.refreshed_at
                    .map_or(0.0, |at| at.elapsed().as_secs_f64()),
            );
            return Ok((Arc::clone(&self.targets), 0, true));
        }

        let started = Instant::now();
        match discover_targets(
            client,
            &mut self.watches,
            pairs,
            inventory,
            max_targets,
            allowed_target_namespaces,
        )
        .await
        {
            Ok(targets) => {
                crate::discovery_metrics::refresh("complete");
                crate::worker_metrics::success("discovery");
                crate::discovery_metrics::cache("refreshed", 0.0);
                let duration_ms = started.elapsed().as_millis() as u64;
                let targets: Arc<[ScrapeTarget]> = targets.into();
                self.targets = Arc::clone(&targets);
                self.refreshed_at = Some(Instant::now());
                Ok((targets, duration_ms, false))
            }
            Err(error) => {
                crate::discovery_metrics::refresh("failed");
                crate::discovery_metrics::cache(
                    "refresh_failed",
                    self.refreshed_at
                        .map_or(0.0, |at| at.elapsed().as_secs_f64()),
                );
                let stale_allowed = self.refreshed_at.is_some_and(|refreshed_at| {
                    refreshed_at.elapsed()
                        < refresh_interval.checked_mul(2).unwrap_or(Duration::MAX)
                });
                if stale_allowed && !self.targets.is_empty() {
                    crate::discovery_metrics::cache(
                        "stale",
                        self.refreshed_at
                            .map_or(0.0, |at| at.elapsed().as_secs_f64()),
                    );
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
    let http = match scrape_client(
        controller.config().scrape_timeout,
        controller.config().scrape_allowed_destinations.clone(),
    ) {
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
                let timestamp = Timestamp::now().display_with_offset(Offset::UTC).to_string();
                match result {
                    Ok((targets, healthy, samples, discovery_duration_ms, discovery_cache_hit)) => {
                        if healthy == targets { crate::worker_metrics::success("scrape"); }
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
                        let _ = crate::queue_metrics::publish(&sender,
                            ScrapeMessage::Complete {
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

fn scrape_client(timeout: Duration, allowed: Vec<String>) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(timeout)
        .user_agent("rush-metrics-agent")
        .redirect(reqwest::redirect::Policy::none())
        // A proxy can resolve targets itself, bypassing SafeResolver.
        .no_proxy()
        .dns_resolver(Arc::new(SafeResolver { allowed }))
        .build()?)
}

async fn scrape_once(
    controller: &Controller,
    http: &reqwest::Client,
    sender: &mpsc::Sender<ScrapeMessage>,
    target_cache: &mut TargetCache,
) -> Result<(usize, usize, u64, u64, bool)> {
    let mut cycle = crate::scrape_metrics::Cycle::default();
    let result = scrape_once_inner(controller, http, sender, target_cache).await;
    cycle.outcome = match &result {
        Ok((targets, healthy, ..)) if targets == healthy => "complete",
        Ok((_, 0, ..)) => "failed",
        Ok(_) => "partial",
        Err(_) => "failed",
    };
    result
}

async fn scrape_once_inner(
    controller: &Controller,
    http: &reqwest::Client,
    sender: &mpsc::Sender<ScrapeMessage>,
    target_cache: &mut TargetCache,
) -> Result<(usize, usize, u64, u64, bool)> {
    let (targets, discovery_duration_ms, discovery_cache_hit) = target_cache
        .get(
            controller.kube_client(),
            controller.resource_pairs(),
            controller
                .scrape_inventory
                .as_deref()
                .context("scrape watch inventory is disabled")?,
            controller.config().scrape_discovery_refresh_interval,
            controller.config().scrape_max_targets,
            &controller.config().scrape_allowed_target_namespaces,
        )
        .await?;
    let internal = internal_target::targets(controller.config(), controller.kube_client()).await;
    let (targets, unavailable_internal_sources) = internal_target::combine(targets, internal);
    crate::scrape_metrics::registry().counter(
        "metrics_agent_internal_discovery_failures_total",
        &[],
        unavailable_internal_sources as u64,
    );
    let limits = ScrapeLimits {
        max_response_bytes: controller.config().scrape_max_response_bytes,
        max_retained_bytes: controller.config().scrape_max_retained_bytes,
        max_samples_per_target: controller.config().scrape_max_samples_per_target,
        max_labels_per_sample: controller.config().scrape_max_labels_per_sample,
        max_label_name_bytes: controller.config().scrape_max_label_name_bytes,
        max_label_value_bytes: controller.config().scrape_max_label_value_bytes,
        max_metric_name_bytes: controller.config().scrape_max_metric_name_bytes,
        max_line_bytes: controller.config().scrape_max_line_bytes,
    };
    let target_count = targets.len();
    let total = target_count.saturating_add(unavailable_internal_sources);
    crate::queue_metrics::publish(sender, ScrapeMessage::Start { targets: total })
        .await
        .context("metrics remote-write channel closed")?;
    let mut results = stream::iter((0..target_count).map(|index| {
        let targets = Arc::clone(&targets);
        async move {
            let target = &targets[index];
            let mut attempt =
                crate::scrape_metrics::Attempt::new(&target.identity.pair, &target.identity.source);
            let result = fetch_target(http, target, controller.config(), limits).await;
            attempt.finish(&result);
            result.map(|metrics| (index, metrics))
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
                if controller.config().ui_enabled {
                    let mut counts = HashMap::new();
                    for metric in &metrics {
                        *counts.entry(metric.name.as_str()).or_insert(0) += 1;
                    }
                    controller
                        .status
                        .record_crd_metrics(
                            identity.key.clone(),
                            identity.pair.clone(),
                            identity.source.clone(),
                            identity.namespace.clone(),
                            identity.name.clone(),
                            metrics.len() as u64,
                            counts.into_iter().map(|(name, series)| MetricCardinality {
                                name: name.to_string(),
                                series,
                            }),
                        )
                        .await;
                }
                samples += send_metric_chunks(sender, metrics).await?;
            }
            Err(error) => debug!(
                reason = error
                    .downcast_ref::<crate::scrape_metrics::Failure>()
                    .map_or("parse_error", |failure| failure.0.label()),
                "scrape target failed"
            ),
        }
    }
    crate::queue_metrics::publish(
        sender,
        ScrapeMessage::Complete {
            targets: total,
            healthy_targets: healthy,
            samples,
            error: None,
        },
    )
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

async fn fetch_target(
    http: &reqwest::Client,
    target: &ScrapeTarget,
    config: &crate::config::Config,
    limits: ScrapeLimits,
) -> Result<Vec<CollectedMetric>> {
    use crate::scrape_metrics::{Failure, Outcome, ResourceLimit};
    if !internal_target::is_internal(target)
        && !namespace_allowed(
            &target.identity.namespace,
            &config.scrape_allowed_namespaces,
        )
    {
        return Err(Failure(Outcome::PolicyDenied).into());
    }
    let url = validate_scrape_url(&target.url, &config.scrape_allowed_destinations)
        .context(Failure(Outcome::PolicyDenied))?;
    let request = internal_target::request(http, target, url, config)
        .await
        .context(Failure(Outcome::ConfigError))?;
    let response = request.send().await.map_err(|error| {
        let outcome = if error.is_timeout() {
            Outcome::Timeout
        } else {
            Outcome::TransportError
        };
        anyhow::Error::new(error).context(Failure(outcome))
    })?;
    if !response.status().is_success() {
        return Err(Failure(Outcome::HttpError).into());
    }
    parse_bounded_response(
        response,
        &target.labels,
        Timestamp::now().as_millisecond(),
        limits,
    )
    .await
    .map_err(|error| {
        let outcome = if error.downcast_ref::<ResourceLimit>().is_some() {
            Outcome::ResourceLimit
        } else if let Some(error) = error.downcast_ref::<reqwest::Error>() {
            if error.is_timeout() {
                Outcome::Timeout
            } else {
                Outcome::TransportError
            }
        } else {
            Outcome::ParseError
        };
        error.context(Failure(outcome))
    })
}

pub(crate) fn namespace_allowed(namespace: &str, allowed: &[String]) -> bool {
    allowed.is_empty()
        || allowed
            .iter()
            .any(|candidate| candidate.trim() == namespace)
}

fn destination_explicitly_allowed(host: &str, allowed: &[String]) -> bool {
    let host = host
        .trim_matches(['[', ']'])
        .trim_end_matches('.')
        .to_ascii_lowercase();
    allowed.iter().any(|candidate| {
        let candidate = candidate
            .trim()
            .trim_matches(['[', ']'])
            .trim_end_matches('.')
            .to_ascii_lowercase();
        if let (Ok(host_ip), Ok(allowed_ip)) = (host.parse::<IpAddr>(), candidate.parse::<IpAddr>())
        {
            return host_ip == allowed_ip;
        }
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
    if let IpAddr::V6(v6) = ip
        && let Some(v4) = v6.to_ipv4()
    {
        return unsafe_destination_ip(IpAddr::V4(v4));
    }
    if kubernetes_service_ip() == Some(ip) {
        return true;
    }
    match ip {
        IpAddr::V4(ip) => {
            ip.is_loopback()
                || ip.is_link_local()
                || ip.octets()[0] == 0
                || ip.is_broadcast()
                || ip.is_multicast()
                || ip == Ipv4Addr::new(169, 254, 169, 254)
                || ip == Ipv4Addr::new(100, 100, 100, 200)
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || (ip.segments()[0] & 0xffc0) == 0xfe80
                || ip == std::net::Ipv6Addr::new(0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254)
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
        .ok_or_else(|| anyhow::anyhow!("scrape target URL has no host"))?
        .trim_matches(['[', ']']);
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
        return Err(crate::scrape_metrics::ResourceLimit(
            "scrape response exceeds the configured byte limit",
        )
        .into());
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
        // Complete lines are drained below, so only the new bytes can hold a
        // newline. Rescanning all of `pending` is quadratic for tiny chunks.
        let start = self.pending.len();
        self.pending.extend_from_slice(chunk);

        let mut consumed = 0usize;
        for newline in self.pending[start..]
            .iter()
            .enumerate()
            .filter_map(|(index, byte)| (*byte == b'\n').then_some(start + index))
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
            return Err(crate::scrape_metrics::ResourceLimit(
                "Prometheus exposition line exceeds the configured byte limit",
            )
            .into());
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<CollectedMetric>> {
        if !self.pending.is_empty() {
            self.parser.push_line(
                std::str::from_utf8(&self.pending).context("scrape response is not valid UTF-8")?,
            )?;
        }
        self.parser.finish()
    }
}

fn account_response_bytes(total: &mut usize, chunk_bytes: usize, max_bytes: usize) -> Result<()> {
    if chunk_bytes > max_bytes.saturating_sub(*total) {
        return Err(crate::scrape_metrics::ResourceLimit(
            "scrape response exceeds the configured byte limit",
        )
        .into());
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
            crate::queue_metrics::publish(
                sender,
                ScrapeMessage::Batch(std::mem::replace(&mut chunk, Vec::with_capacity(10_000))),
            )
            .await
            .context("metrics remote-write channel closed")?;
        }
    }
    if !chunk.is_empty() {
        crate::queue_metrics::publish(sender, ScrapeMessage::Batch(chunk))
            .await
            .context("metrics remote-write channel closed")?;
    }
    Ok(samples)
}

async fn discover_targets(
    client: Client,
    watches: &mut discovery_watch::DiscoveryWatches,
    pairs: &[ResourcePair],
    inventory: &ScrapeInventory,
    max_targets: usize,
    allowed_target_namespaces: &[String],
) -> Result<Vec<ScrapeTarget>> {
    let selected = inventory.selected(pairs).await?;
    let needs = |kind| selected.iter().any(|(pair, _, _)| *pair == kind);
    let mut data = load_discovery_data(
        &client,
        watches,
        needs("service-monitor"),
        needs("pod-monitor"),
    )
    .await?;
    data.allowed_target_namespaces = allowed_target_namespaces.to_vec();
    data.allowed_source_namespaces = inventory.allowed_namespaces().to_vec();
    let mut targets = DiscoveredTargets::new(max_targets);
    for (pair, source, object) in selected {
        targets_for_object(pair, source, &object, &data, &mut targets)?;
    }
    Ok(targets.items)
}

async fn load_discovery_data(
    client: &Client,
    watches: &mut discovery_watch::DiscoveryWatches,
    services_needed: bool,
    pods_needed: bool,
) -> Result<DiscoveryData> {
    let wait = watches.sync_wait;
    let (services, endpoints, pods) = tokio::try_join!(
        watches.services.objects(client, services_needed, wait),
        watches.endpoints.objects(client, services_needed, wait),
        watches.pods.objects(client, pods_needed, wait),
    )?;
    Ok(DiscoveryData {
        services: index_namespaces(services),
        endpoints: endpoints
            .into_iter()
            .map(|object| ((namespace(&object), name(&object)), object))
            .collect(),
        pods: index_namespaces(pods),
        allowed_target_namespaces: Vec::new(),
        allowed_source_namespaces: Vec::new(),
    })
}

fn index_namespaces(objects: Vec<Value>) -> BTreeMap<String, Vec<Value>> {
    let mut index = BTreeMap::<String, Vec<Value>>::new();
    for object in objects {
        index.entry(namespace(&object)).or_default().push(object);
    }
    index
}

fn objects_in_namespaces<'a>(
    index: &'a BTreeMap<String, Vec<Value>>,
    namespaces: &'a [String],
) -> impl Iterator<Item = &'a Value> {
    let all_namespaces = namespaces.iter().any(|name| name == "*");
    let all = all_namespaces
        .then_some(index)
        .into_iter()
        .flat_map(BTreeMap::values);
    let selected = (!all_namespaces)
        .then_some(namespaces)
        .into_iter()
        .flatten()
        .filter_map(|namespace| index.get(namespace));
    all.chain(selected).flatten()
}

struct DiscoveredTargets {
    max: usize,
    urls: HashSet<String>,
    items: Vec<ScrapeTarget>,
}

impl DiscoveredTargets {
    fn new(max: usize) -> Self {
        Self {
            max,
            urls: HashSet::new(),
            items: Vec::new(),
        }
    }

    fn push(&mut self, target: ScrapeTarget) -> Result<()> {
        if self.urls.insert(target.url.clone()) {
            if self.items.len() == self.max {
                bail!(
                    "scrape discovery exceeded the configured limit of {} targets",
                    self.max
                );
            }
            self.items.push(target);
        }
        Ok(())
    }
}

fn targets_for_object(
    pair: &str,
    source: &str,
    object: &ScrapeObject,
    data: &DiscoveryData,
    targets: &mut DiscoveredTargets,
) -> Result<()> {
    let identity = TargetIdentity {
        key: format!("{source}:{pair}:{}/{}", object.namespace, object.name),
        pair: pair.to_string(),
        source: source.to_string(),
        namespace: object.namespace.clone(),
        name: object.name.clone(),
    };
    let spec = &object.spec;
    if matches!(pair, "probe" | "scrape-config") && !arbitrary_hosts_allowed(object, data) {
        warn!(
            source = identity.source,
            kind = identity.pair,
            namespace = identity.namespace,
            name = identity.name,
            "skipping scrape object that can name any host; list its namespace in the allowed scrape namespaces or allow all target namespaces"
        );
        return Ok(());
    }
    match pair {
        "service-monitor" => service_targets(spec, &identity, data, targets),
        "pod-monitor" => pod_targets(spec, &identity, data, targets),
        "probe" => probe_targets(spec, &identity, targets),
        "scrape-config" => static_targets(spec, &identity, targets),
        _ => Ok(()),
    }
}

/// Probe and ScrapeConfig objects name hosts directly, so the per-namespace
/// target restriction cannot apply to them. While that restriction is on, only
/// namespaces the operator listed explicitly may define them.
fn arbitrary_hosts_allowed(object: &ScrapeObject, data: &DiscoveryData) -> bool {
    data.allowed_target_namespaces
        .iter()
        .any(|name| name.trim() == "*")
        || data
            .allowed_source_namespaces
            .iter()
            .any(|name| name.trim() == object.namespace)
}

fn monitor_selection(
    spec: &Value,
    identity: &TargetIdentity,
    data: &DiscoveryData,
) -> Option<(selector::LabelSelector, Vec<String>)> {
    let selection = selector::LabelSelector::parse(spec.get("selector")).and_then(|selector| {
        selector::selected_namespaces(
            spec.get("namespaceSelector"),
            &identity.namespace,
            &data.allowed_target_namespaces,
        )
        .map(|namespaces| (selector, namespaces))
    });
    match selection {
        Ok(selection) => Some(selection),
        Err(error) => {
            warn!(source = identity.source, kind = identity.pair, namespace = identity.namespace, name = identity.name, error = %error, "skipping invalid Kubernetes scrape selector");
            None
        }
    }
}

fn service_targets(
    spec: &Value,
    identity: &TargetIdentity,
    data: &DiscoveryData,
    targets: &mut DiscoveredTargets,
) -> Result<()> {
    let Some((selector, namespaces)) = monitor_selection(spec, identity, data) else {
        return Ok(());
    };
    let default_endpoints = [Value::Null];
    let endpoints = spec
        .get("endpoints")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&default_endpoints);
    for service in objects_in_namespaces(&data.services, &namespaces) {
        if !selector.matches(service.pointer("/metadata/labels")) {
            continue;
        }
        let service_namespace = namespace(service);
        let service_name = name(service);
        let endpoint_data = data
            .endpoints
            .get(&(service_namespace.clone(), service_name.clone()));
        for endpoint in endpoints {
            let path = endpoint
                .get("path")
                .and_then(Value::as_str)
                .unwrap_or("/metrics");
            let scheme = endpoint
                .get("scheme")
                .and_then(Value::as_str)
                .unwrap_or("http");
            let requested_port = endpoint.get("port").or_else(|| endpoint.get("targetPort"));
            for (ip, port, endpoint_namespace) in
                endpoint_addresses(endpoint_data, requested_port, service)
            {
                if endpoint_namespace.is_some_and(|namespace| namespace != service_namespace) {
                    continue;
                }
                let instance = host_port(ip, port);
                let Some(url) = endpoint_url(scheme, ip, port, path, identity) else {
                    continue;
                };
                targets.push(ScrapeTarget {
                    url,
                    labels: vec![
                        ("job".into(), identity.name.clone()),
                        ("namespace".into(), service_namespace.clone()),
                        ("service".into(), service_name.clone()),
                        ("instance".into(), instance),
                    ],
                    identity: identity.clone(),
                })?;
            }
        }
    }
    Ok(())
}

fn pod_targets(
    spec: &Value,
    identity: &TargetIdentity,
    data: &DiscoveryData,
    targets: &mut DiscoveredTargets,
) -> Result<()> {
    let Some((selector, namespaces)) = monitor_selection(spec, identity, data) else {
        return Ok(());
    };
    let default_endpoints = [Value::Null];
    let endpoints = spec
        .get("podMetricsEndpoints")
        .or_else(|| spec.get("endpoints"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&default_endpoints);
    for pod in objects_in_namespaces(&data.pods, &namespaces) {
        if !selector.matches(pod.pointer("/metadata/labels")) {
            continue;
        }
        let Some(ip) = pod
            .get("status")
            .and_then(|v| v.get("podIP"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let pod_namespace = namespace(pod);
        for endpoint in endpoints {
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
            let instance = host_port(ip, port);
            let Some(url) = endpoint_url(scheme, ip, port, path, identity) else {
                continue;
            };
            targets.push(ScrapeTarget {
                url,
                labels: vec![
                    ("job".into(), identity.name.clone()),
                    ("namespace".into(), pod_namespace.clone()),
                    ("pod".into(), name(pod)),
                    ("instance".into(), instance),
                ],
                identity: identity.clone(),
            })?;
        }
    }
    Ok(())
}

fn static_targets(
    spec: &Value,
    identity: &TargetIdentity,
    targets: &mut DiscoveredTargets,
) -> Result<()> {
    let configs = spec
        .get("staticConfigs")
        .or_else(|| spec.get("static_configs"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten();
    let scheme = spec.get("scheme").and_then(Value::as_str).unwrap_or("http");
    let path = spec
        .get("metricsPath")
        .or_else(|| spec.get("path"))
        .and_then(Value::as_str)
        .unwrap_or("/metrics");
    for config in configs {
        let mut labels = config
            .get("labels")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
            .map(|(key, value)| (key.clone(), value.as_str().unwrap_or_default().to_string()))
            .collect::<Vec<_>>();
        if let Some((invalid, _)) = labels.iter().find(|(key, _)| !valid_static_label_name(key)) {
            warn!(source = identity.source, kind = identity.pair, namespace = identity.namespace, name = identity.name, label = %invalid, "skipping static scrape config with an invalid label name");
            continue;
        }
        // The CRD's own namespace always wins so a static label cannot claim
        // another tenant's namespace.
        labels.retain(|(key, _)| key != "namespace");
        for target in config
            .get("targets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            let mut target_labels = labels.clone();
            if !target_labels.iter().any(|(key, _)| key == "job") {
                target_labels.push(("job".into(), identity.name.clone()));
            }
            if !target_labels.iter().any(|(key, _)| key == "instance") {
                target_labels.push(("instance".into(), target.to_string()));
            }
            target_labels.push(("namespace".into(), identity.namespace.clone()));
            targets.push(ScrapeTarget {
                url: normalize_target(target, scheme, path),
                labels: target_labels,
                identity: identity.clone(),
            })?;
        }
    }
    Ok(())
}

/// Prometheus label names, excluding the reserved `__` prefix.
fn valid_static_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
        && !name.starts_with("__")
}

fn probe_targets(
    spec: &Value,
    identity: &TargetIdentity,
    targets: &mut DiscoveredTargets,
) -> Result<()> {
    let prober = spec
        .get("prober")
        .and_then(|v| v.get("url"))
        .and_then(Value::as_str)
        .and_then(|url| Url::parse(url).ok());
    let Some(prober) = prober else { return Ok(()) };
    let addresses = spec
        .get("targets")
        .and_then(|v| v.get("staticConfig"))
        .and_then(|v| v.get("static"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str);
    for target in addresses {
        let mut url = prober.clone();
        url.query_pairs_mut().append_pair("target", target);
        targets.push(ScrapeTarget {
            url: url.to_string(),
            labels: vec![
                ("job".into(), identity.name.clone()),
                ("instance".into(), target.into()),
                ("namespace".into(), identity.namespace.clone()),
            ],
            identity: identity.clone(),
        })?;
    }
    Ok(())
}

fn endpoint_addresses<'a>(
    endpoint_data: Option<&'a Value>,
    requested: Option<&Value>,
    service: &'a Value,
) -> impl Iterator<Item = (&'a str, u16, Option<&'a str>)> {
    let requested = requested.and_then(value_string);
    endpoint_data
        .and_then(|data| data.get("subsets"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .flat_map(move |subset| {
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
                                || number.map(|v| v.to_string()).as_deref() == Some(requested) =>
                        {
                            number
                        }
                        Some(_) => None,
                        None => number,
                    }
                })
                .or_else(|| service_port(service, requested.as_deref()));
            subset
                .get("addresses")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(move |address| {
                    Some((
                        address.get("ip")?.as_str()?,
                        port?,
                        address
                            .pointer("/targetRef/namespace")
                            .and_then(Value::as_str),
                    ))
                })
        })
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

/// `ip:port` as Prometheus writes it: IPv6 is bracketed so the URL and the
/// `instance` label parse.
fn host_port(ip: &str, port: u16) -> String {
    match ip.parse::<IpAddr>() {
        Ok(ip) => std::net::SocketAddr::new(ip, port).to_string(),
        Err(_) => format!("{ip}:{port}"),
    }
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
/// Builds a ServiceMonitor or PodMonitor URL that can only reach the
/// discovered endpoint, whatever the monitor's scheme and path contain.
fn endpoint_url(
    scheme: &str,
    ip: &str,
    port: u16,
    path: &str,
    identity: &TargetIdentity,
) -> Option<String> {
    let scheme = scheme.to_ascii_lowercase();
    let url = matches!(scheme.as_str(), "http" | "https")
        .then(|| build_url(&scheme, &host_port(ip, port), path));
    let points_at_endpoint = url
        .as_deref()
        .and_then(|url| Url::parse(url).ok())
        .is_some_and(|url| {
            url.host_str()
                .zip(url.port_or_known_default())
                .is_some_and(|(host, url_port)| {
                    host_port(host.trim_matches(['[', ']']), url_port) == host_port(ip, port)
                })
        });
    if !points_at_endpoint {
        warn!(
            source = identity.source,
            kind = identity.pair,
            namespace = identity.namespace,
            name = identity.name,
            scheme,
            path,
            "skipping Kubernetes scrape endpoint whose scheme or path does not address the discovered endpoint"
        );
        return None;
    }
    url
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
#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use serde_json::json;
    use tokio::sync::mpsc;

    use super::{
        BoundedTextParser, DiscoveredTargets, DiscoveryData, ScrapeObject, ScrapeTarget,
        TargetIdentity, account_response_bytes, destination_explicitly_allowed, index_namespaces,
        namespace_allowed, pod_targets, probe_targets, send_metric_chunks, service_targets,
        static_targets, targets_for_object, unsafe_destination_ip, validate_scrape_url,
    };
    use crate::remote_write::{CollectedMetric, MetricType, ScrapeLimits};
    use crate::status::{MetricCardinality, StatusStore};

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

    #[test]
    fn ipv6_literals_cannot_bypass_destination_policy() {
        for host in [
            "[::1]",
            "[::]",
            "[fe80::1]",
            "[ff02::1]",
            "[::ffff:127.0.0.1]",
            "[::ffff:169.254.169.254]",
        ] {
            assert!(
                validate_scrape_url(&format!("http://{host}:9090/metrics"), &[]).is_err(),
                "{host}"
            );
        }
    }

    #[test]
    fn mapped_dns_answers_are_blocked_but_private_cluster_ips_work() {
        for ip in [
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            "::ffff:0.0.0.0",
            "::127.0.0.1",
        ] {
            assert!(unsafe_destination_ip(ip.parse().unwrap()), "{ip}");
        }
        for host in ["[fd00::20]", "[::ffff:10.0.0.20]"] {
            assert!(validate_scrape_url(&format!("http://{host}/metrics"), &[]).is_ok());
        }
        assert!(validate_scrape_url("http://[::1]/metrics", &["::1".into()]).is_ok());
        assert!(validate_scrape_url("http://[::1]/metrics", &["[0:0:0:0:0:0:0:1]".into()]).is_ok());
    }

    #[test]
    fn cloud_metadata_addresses_are_denied_in_both_ip_families() {
        for host in [
            "169.254.169.254",
            "100.100.100.200",
            "[fd00:ec2::254]",
            "[::ffff:100.100.100.200]",
        ] {
            assert!(
                validate_scrape_url(&format!("http://{host}/metrics"), &[]).is_err(),
                "{host}"
            );
            assert!(unsafe_destination_ip(
                host.trim_matches(['[', ']']).parse().unwrap()
            ));
        }
    }

    #[tokio::test]
    async fn scrape_proxy_child() {
        if std::env::var_os("METRICS_AGENT_PROXY_TEST_CHILD").is_none() {
            return;
        }
        let client = super::scrape_client(std::time::Duration::from_secs(1), vec![]).unwrap();
        // This literal has no DNS lookup. A proxy would otherwise receive the request.
        let _ = client.get("http://127.0.0.1:1/metrics").send().await;
    }

    #[tokio::test]
    async fn scraper_ignores_environment_proxies() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = format!("http://{}", listener.local_addr().unwrap());
        let exe = std::env::current_exe().unwrap();
        let child = tokio::task::spawn_blocking(move || {
            std::process::Command::new(exe)
                .args(["--exact", "scraper::tests::scrape_proxy_child"])
                .env("METRICS_AGENT_PROXY_TEST_CHILD", "1")
                .env("HTTP_PROXY", &proxy)
                .env("http_proxy", &proxy)
                .env("ALL_PROXY", &proxy)
                .env("all_proxy", &proxy)
                .env("NO_PROXY", "")
                .env("no_proxy", "")
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        assert!(
            child.status.success(),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    }

    fn discovery_data() -> DiscoveryData {
        DiscoveryData {
            allowed_target_namespaces: vec!["default".into()],
            allowed_source_namespaces: Vec::new(),
            services: index_namespaces(vec![json!({
                "metadata": {
                    "name": "api",
                    "namespace": "default",
                    "labels": {"app": "api"}
                },
                "spec": {"ports": [{"name": "metrics", "port": 9100}]}
            })]),
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
            pods: index_namespaces(vec![json!({
                "metadata": {
                    "name": "api-pod",
                    "namespace": "default",
                    "labels": {"app": "api"}
                },
                "status": {"podIP": "10.0.0.2"},
                "spec": {"containers": [{"ports": [{"name": "metrics", "containerPort": 8080}]}]}
            })]),
        }
    }

    #[test]
    fn builds_service_monitor_targets_with_selector_namespace_and_labels() {
        let spec = json!({
            "selector": {"matchLabels": {"app": "api"}},
            "namespaceSelector": {"matchNames": ["default"]},
            "endpoints": [{"port": "metrics", "scheme": "https", "path": "custom"}]
        });
        let mut targets = DiscoveredTargets::new(1);
        service_targets(
            &spec,
            &identity("service-monitor", "victoria"),
            &discovery_data(),
            &mut targets,
        )
        .unwrap();
        let targets = targets.items;
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

    fn discovery_data_at(ip: &str, port: u16) -> DiscoveryData {
        let mut data = discovery_data();
        let endpoints = data
            .endpoints
            .get_mut(&("default".into(), "api".into()))
            .unwrap();
        endpoints["subsets"][0]["addresses"][0]["ip"] = json!(ip);
        endpoints["subsets"][0]["ports"][0]["port"] = json!(port);
        let pod = &mut data.pods.get_mut("default").unwrap()[0];
        pod["status"]["podIP"] = json!(ip);
        pod["spec"]["containers"][0]["ports"][0]["containerPort"] = json!(port);
        data
    }

    /// One ServiceMonitor target and one PodMonitor target, expanded separately
    /// because both point at the same address and would be deduplicated.
    fn endpoint_and_pod_targets(data: &DiscoveryData) -> Vec<ScrapeTarget> {
        let spec = json!({
            "selector": {"matchLabels": {"app": "api"}},
            "namespaceSelector": {"matchNames": ["default"]},
            "endpoints": [{"port": "metrics"}]
        });
        let mut service = DiscoveredTargets::new(1);
        service_targets(
            &spec,
            &identity("service-monitor", "prometheus"),
            data,
            &mut service,
        )
        .unwrap();
        let mut pod = DiscoveredTargets::new(1);
        pod_targets(
            &spec,
            &identity("pod-monitor", "prometheus"),
            data,
            &mut pod,
        )
        .unwrap();
        let targets = [service.items, pod.items].concat();
        assert_eq!(targets.len(), 2);
        targets
    }

    #[tokio::test]
    async fn ipv6_endpoints_and_pods_are_bracketed_and_scrapeable() {
        use clap::Parser;
        for target in endpoint_and_pod_targets(&discovery_data_at("fd00::1", 8080)) {
            assert_eq!(target.url, "http://[fd00::1]:8080/metrics");
            assert!(
                target
                    .labels
                    .contains(&("instance".into(), "[fd00::1]:8080".into()))
            );
            assert!(validate_scrape_url(&target.url, &[]).is_ok());
        }

        let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().route("/metrics", axum::routing::get(|| async { "up 1\n" })),
            )
            .await
            .unwrap();
        });
        let mut config = crate::config::Config::parse_from(["test"]);
        config.scrape_allowed_destinations = vec!["::1".into()];
        let http =
            super::scrape_client(std::time::Duration::from_secs(2), vec!["::1".into()]).unwrap();
        for target in endpoint_and_pod_targets(&discovery_data_at("::1", port)) {
            assert_eq!(target.url, format!("http://[::1]:{port}/metrics"));
            let samples = super::fetch_target(&http, &target, &config, ScrapeLimits::default())
                .await
                .unwrap();
            assert_eq!(samples[0].name, "up");
        }
        server.abort();
    }

    #[tokio::test]
    async fn bracketed_ipv6_targets_still_hit_the_destination_policy() {
        use clap::Parser;
        let config = crate::config::Config::parse_from(["test"]);
        let http = super::scrape_client(std::time::Duration::from_secs(1), vec![]).unwrap();
        for ip in ["fd00:ec2::254", "fe80::1", "::1", "::ffff:169.254.169.254"] {
            for target in endpoint_and_pod_targets(&discovery_data_at(ip, 8080)) {
                assert_eq!(target.url, format!("http://[{ip}]:8080/metrics"));
                assert!(validate_scrape_url(&target.url, &[]).is_err(), "{ip}");
                let error = super::fetch_target(&http, &target, &config, ScrapeLimits::default())
                    .await
                    .unwrap_err();
                assert!(
                    matches!(
                        error.downcast_ref::<crate::scrape_metrics::Failure>(),
                        Some(crate::scrape_metrics::Failure(
                            crate::scrape_metrics::Outcome::PolicyDenied
                        ))
                    ),
                    "{ip}"
                );
            }
        }
    }

    #[test]
    fn builds_pod_monitor_targets_from_named_container_ports() {
        let spec = json!({
            "selector": {"matchLabels": {"app": "api"}},
            "namespaceSelector": {"any": true},
            "podMetricsEndpoints": [{"port": "metrics", "scheme": "https", "path": "/pod-metrics"}]
        });
        let mut targets = DiscoveredTargets::new(1);
        pod_targets(
            &spec,
            &identity("pod-monitor", "prometheus"),
            &discovery_data(),
            &mut targets,
        )
        .unwrap();
        let targets = targets.items;
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
    fn monitor_discovery_intersects_expression_and_destination_policy() {
        let mut data = discovery_data();
        type Expand = fn(
            &serde_json::Value,
            &TargetIdentity,
            &DiscoveryData,
            &mut DiscoveredTargets,
        ) -> anyhow::Result<()>;
        let monitors: [(&str, Expand); 2] = [
            ("service-monitor", service_targets),
            ("pod-monitor", pod_targets),
        ];
        for (kind, expand) in monitors {
            let identity = identity(kind, "prometheus");
            let make_spec = |expression| {
                json!({
                    "namespaceSelector": {"any": true},
                    "selector": {"matchExpressions": [expression]},
                })
            };
            let mut targets = DiscoveredTargets::new(2);
            data.allowed_target_namespaces = vec!["default".into()];
            let different_app = make_spec(json!({"key":"app","operator":"In","values":["other"]}));
            expand(&different_app, &identity, &data, &mut targets).unwrap();
            assert!(targets.items.is_empty());
            let matching_app = make_spec(json!({"key":"app","operator":"In","values":["api"]}));
            data.allowed_target_namespaces.clear();
            expand(&matching_app, &identity, &data, &mut targets).unwrap();
            assert!(targets.items.is_empty());
            data.allowed_target_namespaces.push("default".into());
            expand(&matching_app, &identity, &data, &mut targets).unwrap();
            assert_eq!(targets.items.len(), 1);
        }
    }

    #[test]
    fn builds_probe_targets_with_encoded_query_parameters() {
        let spec = json!({
            "prober": {"url": "http://blackbox:9115/probe?module=http_2xx"},
            "targets": {"staticConfig": {"static": ["https://example.com/a?x=1", "http://internal"]}}
        });
        let mut targets = DiscoveredTargets::new(2);
        probe_targets(&spec, &identity("probe", "victoria"), &mut targets).unwrap();
        let targets = targets.items;
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
        assert!(
            targets[0]
                .labels
                .contains(&("namespace".into(), "monitoring".into()))
        );
    }

    #[test]
    fn static_labels_cannot_spoof_namespace_or_use_invalid_names() {
        let spec = json!({
            "staticConfigs": [
                {"targets": ["node-a:9100"], "labels": {"namespace": "payments", "team": "a"}},
                {"targets": ["node-b:9100"], "labels": {"__address__": "x"}},
                {"targets": ["node-c:9100"], "labels": {"bad-name": "x"}}
            ]
        });
        let mut targets = DiscoveredTargets::new(3);
        static_targets(&spec, &identity("scrape-config", "victoria"), &mut targets).unwrap();
        let targets = targets.items;
        assert_eq!(targets.len(), 1);
        let namespaces = targets[0]
            .labels
            .iter()
            .filter(|(key, _)| key == "namespace")
            .collect::<Vec<_>>();
        assert_eq!(namespaces, [&("namespace".into(), "monitoring".into())]);
        assert!(targets[0].labels.contains(&("team".into(), "a".into())));
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
        let mut targets = DiscoveredTargets::new(2);
        static_targets(&spec, &identity("scrape-config", "victoria"), &mut targets).unwrap();
        let targets = targets.items;
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
        let object = ScrapeObject {
            namespace: "monitoring".into(),
            name: "api-monitor".into(),
            spec: json!({"selector": {"matchLabels": {"app": "api"}}, "namespaceSelector": {"matchNames": ["default"]}}),
        };
        let mut targets = DiscoveredTargets::new(1);
        targets_for_object(
            "service-monitor",
            "prometheus",
            &object,
            &discovery_data(),
            &mut targets,
        )
        .unwrap();
        let targets = targets.items;
        assert_eq!(
            targets[0].identity.key,
            "prometheus:service-monitor:monitoring/api-monitor"
        );
    }

    #[test]
    fn monitor_scheme_and_path_cannot_redirect_to_another_host() {
        type Expand = fn(
            &serde_json::Value,
            &TargetIdentity,
            &DiscoveryData,
            &mut DiscoveredTargets,
        ) -> anyhow::Result<()>;
        let monitors: [(&str, &str, &str, Expand); 2] = [
            (
                "service-monitor",
                "endpoints",
                "10.0.0.1:9100",
                service_targets,
            ),
            (
                "pod-monitor",
                "podMetricsEndpoints",
                "10.0.0.2:8080",
                pod_targets,
            ),
        ];
        for (kind, field, endpoint_address, expand) in monitors {
            let discover = |scheme: &str, path: &str| {
                let spec = json!({
                    "selector": {"matchLabels": {"app": "api"}},
                    "namespaceSelector": {"matchNames": ["default"]},
                    field: [{"port": "metrics", "scheme": scheme, "path": path}]
                });
                let mut targets = DiscoveredTargets::new(2);
                expand(
                    &spec,
                    &identity(kind, "prometheus"),
                    &discovery_data(),
                    &mut targets,
                )
                .unwrap();
                targets.items
            };
            for scheme in [
                "http://10.4.2.9:9100/metrics?x=",
                "http://10.4.2.9:9100/metrics#",
                "https://10.4.2.9:9100",
                "file",
                "",
            ] {
                assert!(discover(scheme, "/metrics").is_empty(), "{kind} {scheme:?}");
            }
            for (scheme, expected) in [("http", "http"), ("HTTPS", "https")] {
                let targets = discover(scheme, "/metrics");
                assert_eq!(targets.len(), 1, "{kind} {scheme}");
                assert_eq!(
                    targets[0].url,
                    format!("{expected}://{endpoint_address}/metrics")
                );
            }
            // The path is always rooted after the endpoint, so it cannot change the authority.
            for path in [
                "@10.4.2.9:9100/metrics",
                "//10.4.2.9:9100/metrics",
                "\\\\10.4.2.9:9100/metrics",
                ":81/metrics",
            ] {
                for target in discover("http", path) {
                    let url = reqwest::Url::parse(&target.url).unwrap();
                    assert_eq!(
                        format!("{}:{}", url.host_str().unwrap(), url.port().unwrap()),
                        endpoint_address,
                        "{kind} {path:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn endpoint_url_requires_the_url_to_address_the_endpoint() {
        let identity = identity("service-monitor", "prometheus");
        assert_eq!(
            super::endpoint_url("http", "10.0.0.1", 80, "metrics", &identity).as_deref(),
            Some("http://10.0.0.1:80/metrics")
        );
        assert!(super::endpoint_url("http", "evil.example/x?", 80, "/", &identity).is_none());
        assert!(super::endpoint_url("http", "10.0.0.1:81/x?", 80, "/", &identity).is_none());
    }

    #[test]
    fn host_naming_objects_need_an_explicitly_allowed_namespace() {
        let probe = ScrapeObject {
            namespace: "monitoring".into(),
            name: "probe".into(),
            spec: json!({
                "prober": {"url": "http://blackbox:9115/probe"},
                "targets": {"staticConfig": {"static": ["10.4.2.9:9100"]}}
            }),
        };
        let scrape_config = ScrapeObject {
            namespace: "monitoring".into(),
            name: "static".into(),
            spec: json!({"staticConfigs": [{"targets": ["10.4.2.9:9100"]}]}),
        };
        let discovered = |data: &DiscoveryData| {
            let mut targets = DiscoveredTargets::new(4);
            targets_for_object("probe", "prometheus", &probe, data, &mut targets).unwrap();
            targets_for_object(
                "scrape-config",
                "victoria",
                &scrape_config,
                data,
                &mut targets,
            )
            .unwrap();
            targets.items.len()
        };

        let mut data = discovery_data();
        // Restriction on, source namespaces unrestricted or not listing it.
        data.allowed_target_namespaces.clear();
        assert_eq!(discovered(&data), 0);
        data.allowed_target_namespaces = vec!["default".into()];
        data.allowed_source_namespaces = vec!["apps".into()];
        assert_eq!(discovered(&data), 0);
        // Restriction on, namespace listed explicitly.
        data.allowed_source_namespaces = vec!["apps".into(), " monitoring ".into()];
        assert_eq!(discovered(&data), 2);
        // Restriction off.
        data.allowed_source_namespaces.clear();
        data.allowed_target_namespaces = vec!["*".into()];
        assert_eq!(discovered(&data), 2);
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
        let mut targets = DiscoveredTargets::new(1);
        let mut overflow = second.clone();
        overflow.url = "http://10.0.0.2:9100/metrics".into();
        targets.push(first).unwrap();
        targets.push(second).unwrap();
        assert!(targets.push(overflow).is_err());
        assert_eq!(targets.items.len(), 1);
        assert_eq!(targets.items[0].labels[0].1, "first");
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
    fn streaming_parser_handles_long_lines_fed_one_byte_at_a_time() {
        let value = "v".repeat(4_000);
        let body = format!(
            "# {}\r\nlong{{label=\"{value}\"}} 7\r\nshort 8\n",
            "x".repeat(60_000)
        );
        let mut parser = BoundedTextParser::new(&[], 42, ScrapeLimits::default()).unwrap();
        for byte in body.as_bytes() {
            parser.push_chunk(std::slice::from_ref(byte)).unwrap();
        }

        let metrics = parser.finish().unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].name, "long");
        assert_eq!(metrics[0].labels[0].1, value);
        assert_eq!(metrics[0].value, 7.0);
        assert_eq!(metrics[1].name, "short");
    }

    #[test]
    fn streaming_parser_scans_each_byte_once() {
        // A full rescan per chunk costs about 4.5e10 byte checks here (minutes
        // even in release builds); a linear scan finishes in milliseconds.
        let limits = ScrapeLimits {
            max_line_bytes: 300_000,
            ..ScrapeLimits::default()
        };
        let line = format!("# {}\n", "x".repeat(299_000));
        let mut parser = BoundedTextParser::new(&[], 42, limits).unwrap();
        let started = std::time::Instant::now();
        for byte in line.as_bytes() {
            parser.push_chunk(std::slice::from_ref(byte)).unwrap();
        }
        assert!(parser.pending.is_empty());
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "byte-at-a-time line took {:?}",
            started.elapsed()
        );
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
    #[tokio::test]
    async fn failed_crd_refresh_preserves_previous_targets_until_stale_deadline() {
        use std::{
            sync::Arc,
            time::{Duration, Instant},
        };

        let client =
            kube::Client::try_from(kube::Config::new("http://127.0.0.1:1".parse().unwrap()))
                .unwrap();
        let mut cache = super::TargetCache {
            refreshed_at: Some(Instant::now() - Duration::from_secs(15)),
            targets: vec![ScrapeTarget {
                url: "http://old:80/metrics".into(),
                labels: vec![],
                identity: identity("service-monitor", "victoria"),
            }]
            .into(),
            watches: Default::default(),
        };
        let pairs = crate::controller::resource_pairs();
        let inventory = crate::controller::ScrapeInventory::new(pairs.len(), &[]);
        let (retained, _, stale) = cache
            .get(
                client.clone(),
                &pairs,
                &inventory,
                Duration::from_secs(10),
                1,
                &[],
            )
            .await
            .unwrap();
        assert!(stale);
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].url, "http://old:80/metrics");
        cache.refreshed_at = Some(Instant::now() - Duration::from_secs(21));
        assert!(
            cache
                .get(client, &pairs, &inventory, Duration::from_secs(10), 1, &[])
                .await
                .is_err()
        );
        assert!(Arc::ptr_eq(&retained, &cache.targets));
    }

    #[tokio::test]
    async fn core_discovery_lists_once_then_reads_the_watch_cache() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        use axum::{Json, Router, body::Body, http::Uri, response::IntoResponse};
        let lists = Arc::new(AtomicUsize::new(0));
        let server_lists = Arc::clone(&lists);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().fallback(move |uri: Uri| {
                let lists = Arc::clone(&server_lists);
                async move {
                    assert_eq!(uri.path(), "/api/v1/pods");
                    let query = uri.query().unwrap();
                    if query.contains("watch=true") {
                        // An open watch with no changes.
                        let idle = futures_util::stream::pending::<Result<Vec<u8>, std::io::Error>>();
                        return Body::from_stream(idle).into_response();
                    }
                    lists.fetch_add(1, Ordering::SeqCst);
                    assert!(query.contains("limit=500"));
                    let (name, token) = if query.contains("continue=second") {
                        ("pod-b", "")
                    } else {
                        ("pod-a", "second")
                    };
                    Json(json!({"apiVersion":"v1","kind":"PodList","metadata":{"continue": token, "resourceVersion": "1"},
                        "items":[{"apiVersion":"v1","kind":"Pod","metadata":{"name":name,"namespace":"apps"}}]}))
                    .into_response()
                }
            })).await.unwrap();
        });
        let client = kube::Client::try_from(kube::Config::new(endpoint.parse().unwrap())).unwrap();
        let mut watches = super::discovery_watch::DiscoveryWatches::default();
        for _ in 0..3 {
            let data = super::load_discovery_data(&client, &mut watches, false, true)
                .await
                .unwrap();
            assert!(data.services.is_empty());
            assert!(data.endpoints.is_empty());
            assert_eq!(
                data.pods["apps"]
                    .iter()
                    .map(super::name)
                    .collect::<Vec<_>>(),
                ["pod-a", "pod-b"]
            );
        }
        // Two pages of one initial list; later refreshes read the cache.
        assert_eq!(lists.load(Ordering::SeqCst), 2);
        assert!(watches.services.running.is_none() && watches.endpoints.running.is_none());

        let data = super::load_discovery_data(&client, &mut watches, false, false)
            .await
            .unwrap();
        assert!(data.pods.is_empty());
        assert!(watches.pods.running.is_none());
        server.abort();
    }

    #[tokio::test]
    async fn unsynchronized_core_watch_fails_discovery_instead_of_finding_no_pods() {
        use axum::{
            Router,
            http::{StatusCode, Uri},
            response::IntoResponse,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(|uri: Uri| async move {
                    if uri.path() == "/api/v1/pods" {
                        // An initial list that never completes.
                        std::future::pending::<()>().await;
                    }
                    (StatusCode::FORBIDDEN, "denied").into_response()
                }),
            )
            .await
            .unwrap();
        });
        let client = kube::Client::try_from(kube::Config::new(endpoint.parse().unwrap())).unwrap();
        let mut watches = super::discovery_watch::DiscoveryWatches {
            sync_wait: std::time::Duration::from_millis(200),
            ..Default::default()
        };
        let Err(error) = super::load_discovery_data(&client, &mut watches, false, true).await
        else {
            panic!("an unsynchronized Pod cache must not report an empty cluster");
        };
        assert!(error.to_string().contains("Pod watch is not synchronized"));
        let Err(error) = super::load_discovery_data(&client, &mut watches, true, false).await
        else {
            panic!("a failed Service watch must fail discovery");
        };
        assert!(error.to_string().contains("watch failed"), "{error}");
        server.abort();
    }

    #[tokio::test]
    async fn watched_discovery_matches_list_discovery_targets() {
        use axum::{Json, Router, body::Body, http::Uri, response::IntoResponse};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().fallback(|uri: Uri| async move {
                if uri.query().unwrap_or_default().contains("watch=true") {
                    let idle = futures_util::stream::pending::<Result<Vec<u8>, std::io::Error>>();
                    return Body::from_stream(idle).into_response();
                }
                let meta = |name: &str, labels: serde_json::Value| json!({
                    "name": name, "namespace": "apps", "labels": labels,
                    "annotations": {"note": "dropped"}, "uid": name, "resourceVersion": "1"
                });
                let (kind, items) = match uri.path() {
                    "/api/v1/services" => ("ServiceList", json!([
                        {"metadata": meta("api", json!({"app": "api"})),
                         "spec": {"clusterIP": "10.1.0.1", "ports": [{"name": "metrics", "port": 9100, "targetPort": 8080}]}},
                        {"metadata": meta("other", json!({"app": "other"})),
                         "spec": {"ports": [{"name": "metrics", "port": 9100}]}}
                    ])),
                    "/api/v1/endpoints" => ("EndpointsList", json!([
                        {"metadata": meta("api", json!({"app": "api"})),
                         "subsets": [{"addresses": [{"ip": "10.0.0.1", "targetRef": {"kind": "Pod", "namespace": "apps", "name": "api-1"}},
                                                    {"ip": "10.0.0.2"}],
                                      "ports": [{"name": "metrics", "port": 8080}]}]}
                    ])),
                    "/api/v1/pods" => ("PodList", json!([
                        {"metadata": meta("api-1", json!({"app": "api"})),
                         "spec": {"nodeName": "node-a", "containers": [
                             {"name": "app", "image": "app:1", "env": [{"name": "A", "value": "b"}],
                              "ports": [{"name": "metrics", "containerPort": 8080}]},
                             {"name": "sidecar", "image": "sidecar:1"}]},
                         "status": {"podIP": "10.0.0.1", "phase": "Running"}},
                        {"metadata": meta("api-0", json!({"app": "api"})),
                         "spec": {"containers": [{"name": "app", "ports": [{"name": "metrics", "containerPort": 8080}]}]},
                         "status": {"podIP": "10.0.0.3"}},
                        {"metadata": meta("pending", json!({"app": "api"})),
                         "spec": {"containers": [{"name": "app"}]}, "status": {"phase": "Pending"}}
                    ])),
                    path => panic!("unexpected request {path}"),
                };
                Json(json!({"apiVersion": "v1", "kind": kind, "metadata": {"resourceVersion": "1"}, "items": items}))
                    .into_response()
            })).await.unwrap();
        });
        let client = kube::Client::try_from(kube::Config::new(endpoint.parse().unwrap())).unwrap();
        let mut watches = super::discovery_watch::DiscoveryWatches::default();
        let mut data = super::load_discovery_data(&client, &mut watches, true, true)
            .await
            .unwrap();
        data.allowed_target_namespaces = vec!["apps".into()];
        let mut targets = DiscoveredTargets::new(100);
        for pair in ["service-monitor", "pod-monitor"] {
            let object = ScrapeObject {
                namespace: "monitoring".into(),
                name: pair.into(),
                spec: json!({
                    "selector": {"matchLabels": {"app": "api"}},
                    "namespaceSelector": {"matchNames": ["apps"]},
                    "endpoints": [{"port": "metrics"}],
                }),
            };
            targets_for_object(pair, "prometheus", &object, &data, &mut targets).unwrap();
        }
        let targets = targets
            .items
            .iter()
            .map(|target| (target.url.as_str(), target.labels.clone()))
            .collect::<Vec<_>>();
        let labels = |job: &str, kind: &str, name: &str, instance: &str| {
            vec![
                ("job".to_string(), job.to_string()),
                ("namespace".into(), "apps".into()),
                (kind.into(), name.into()),
                ("instance".into(), instance.into()),
            ]
        };
        // Expected values were produced by the list-based discovery this replaced.
        assert_eq!(
            targets,
            [
                (
                    "http://10.0.0.1:8080/metrics",
                    labels("service-monitor", "service", "api", "10.0.0.1:8080")
                ),
                (
                    "http://10.0.0.2:8080/metrics",
                    labels("service-monitor", "service", "api", "10.0.0.2:8080")
                ),
                (
                    "http://10.0.0.3:8080/metrics",
                    labels("pod-monitor", "pod", "api-0", "10.0.0.3:8080")
                ),
            ]
        );
        server.abort();
    }

    #[tokio::test]
    async fn scrape_failures_are_typed_and_internal_source_is_bounded() {
        use axum::{
            Router,
            http::{StatusCode, Uri},
        };
        use clap::Parser;
        use std::time::Duration;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().fallback(|uri: Uri| async move {
                    match uri.path() {
                        "/http" => (StatusCode::FORBIDDEN, "denied"),
                        "/limit" => (StatusCode::OK, "large_response 100"),
                        "/parse" => (StatusCode::OK, "m{a=bad} 1"),
                        "/timeout" => {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            (StatusCode::OK, "m 1")
                        }
                        _ => (StatusCode::OK, "m 1"),
                    }
                }),
            )
            .await
            .unwrap();
        });
        let mut config = crate::config::Config::parse_from(["test"]);
        config.scrape_allowed_destinations = vec!["127.0.0.1".into()];
        let http =
            super::scrape_client(Duration::from_millis(30), vec!["127.0.0.1".into()]).unwrap();
        for (path, expected, max_bytes) in [
            ("/http", "http_error", 100),
            ("/limit", "resource_limit", 3),
            ("/parse", "parse_error", 100),
            ("/timeout", "timeout", 100),
        ] {
            let target = ScrapeTarget {
                url: format!("{endpoint}{path}"),
                labels: vec![],
                identity: identity("service-monitor", "prometheus"),
            };
            let mut attempt =
                crate::scrape_metrics::Attempt::new(&target.identity.pair, &target.identity.source);
            let result = super::fetch_target(
                &http,
                &target,
                &config,
                ScrapeLimits {
                    max_response_bytes: max_bytes,
                    ..Default::default()
                },
            )
            .await;
            assert_eq!(
                result
                    .as_ref()
                    .unwrap_err()
                    .downcast_ref::<crate::scrape_metrics::Failure>()
                    .unwrap()
                    .0
                    .label(),
                expected
            );
            attempt.finish(&result);
        }
        drop(crate::scrape_metrics::Attempt::new("internal", "internal"));
        drop(crate::scrape_metrics::Attempt::new(
            "arbitrary-name",
            "arbitrary-source",
        ));
        let metrics = crate::scrape_metrics::registry().render();
        assert!(
            metrics
                .contains("resource_pair=\"internal\",source=\"internal\",outcome=\"cancelled\"")
        );
        assert!(!metrics.contains("arbitrary-name"));
        server.abort();
    }
}
