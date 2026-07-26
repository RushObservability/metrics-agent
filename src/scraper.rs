//! Lightweight Prometheus scraping from the Kubernetes scrape CRDs.
//!
//! This intentionally implements the common ServiceMonitor/PodMonitor and
//! static target shapes directly instead of depending on vmagent or
//! VictoriaMetrics. The controller remains useful when those operators are
//! not installed, while native VM scrape objects can still be used as input.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result};
use futures_util::{StreamExt, stream};
use kube::{
    Client,
    api::{Api, ApiResource, DynamicObject, ListParams, ResourceExt},
    core::GroupVersionKind,
};
use reqwest::Url;
use serde_json::Value;
use tokio::{sync::mpsc, time};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    controller::{Controller, ResourcePair},
    remote_write::{CollectedMetric, ScrapeMessage, parse_prometheus_text},
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

pub async fn run(
    controller: Arc<Controller>,
    sender: mpsc::Sender<ScrapeMessage>,
    shutdown: CancellationToken,
) {
    let http = match reqwest::Client::builder()
        .timeout(controller.config().scrape_timeout)
        .user_agent("rush-metrics-agent")
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            warn!(error = %error, "unable to build scrape HTTP client");
            return;
        }
    };
    let mut interval = time::interval(controller.config().scrape_interval);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = interval.tick() => {
                controller.status.begin_scrape().await;
                let result = scrape_once(&controller, &http, &sender).await;
                let timestamp = chrono::Utc::now().to_rfc3339();
                match result {
                    Ok((targets, healthy, samples)) => {
                        controller.status.set_scrape_status(ScrapeStatus {
                            enabled: true,
                            targets: targets as u64,
                            healthy_targets: healthy as u64,
                            samples,
                            errors: targets.saturating_sub(healthy) as u64,
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
) -> Result<(usize, usize, u64)> {
    let targets = discover_targets(controller.kube_client(), controller.resource_pairs()).await?;
    let total = targets.len();
    sender
        .send(ScrapeMessage::Start { targets: total })
        .await
        .context("metrics remote-write channel closed")?;
    let mut results = stream::iter(targets.into_iter().map(|target| async move {
        let identity = target.identity.clone();
        let labels = target.labels.clone();
        let response = http.get(&target.url).send().await?.error_for_status()?;
        let body = response.text().await?;
        let metrics = parse_prometheus_text(&body, &labels, chrono::Utc::now().timestamp_millis());
        Ok::<_, anyhow::Error>((identity, metrics))
    }))
    .buffer_unordered(8);
    let mut healthy = 0;
    let mut samples = 0;
    while let Some(result) = results.next().await {
        match result {
            Ok((identity, metrics)) => {
                healthy += 1;
                let target_samples = metrics.len() as u64;
                let mut counts = HashMap::new();
                for metric in &metrics {
                    *counts.entry(metric.name.clone()).or_insert(0) += 1;
                }
                controller
                    .status
                    .record_crd_metrics(
                        identity.key,
                        identity.pair,
                        identity.source,
                        identity.namespace,
                        identity.name,
                        target_samples,
                        counts
                            .into_iter()
                            .map(|(name, series)| MetricCardinality { name, series }),
                    )
                    .await;
                samples += send_metric_chunks(sender, metrics).await?;
            }
            Err(error) => debug!(error = %error, "scrape target failed"),
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
    Ok((total, healthy, samples))
}

async fn send_metric_chunks(
    sender: &mpsc::Sender<ScrapeMessage>,
    mut metrics: Vec<CollectedMetric>,
) -> Result<u64> {
    let mut samples = 0;
    while !metrics.is_empty() {
        let split_at = metrics.len().min(10_000);
        let remainder = metrics.split_off(split_at);
        samples += split_at as u64;
        sender
            .send(ScrapeMessage::Batch(metrics))
            .await
            .context("metrics remote-write channel closed")?;
        metrics = remainder;
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
    use serde_json::json;
    use tokio::sync::mpsc;

    use super::{
        DiscoveryData, ScrapeTarget, TargetIdentity, deduplicate_targets, pod_targets,
        probe_targets, send_metric_chunks, service_targets, static_targets, targets_for_object,
    };
    use crate::remote_write::{CollectedMetric, MetricType};
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
