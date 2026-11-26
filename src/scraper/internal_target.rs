//! Credentials for this explicit operator-configured target never reach CRD targets.
use anyhow::{Context, Result, bail};
use k8s_openapi::{
    api::{
        core::v1::{Pod, Service, ServicePort},
        discovery::v1::EndpointSlice,
    },
    apimachinery::pkg::util::intstr::IntOrString,
};
use kube::{Api, api::ListParams};
use reqwest::{Client, RequestBuilder, Url, header::HeaderValue};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

use super::{ScrapeTarget, TargetIdentity, validate_scrape_url};
use crate::config::Config;

const TARGET_KEY: &str = "internal/sre-agent";
const MAX_TOKEN_BYTES: u64 = 4096;
const MAX_ENDPOINTS: usize = 256;

/// Keep unrelated workloads flowing when the optional internal source is down.
/// One unresolved source contributes a failure to the cycle, without a fallback
/// request through a load-balanced Service that could interleave pod counters.
pub(super) fn combine(
    existing: Arc<[ScrapeTarget]>,
    internal: Result<Vec<ScrapeTarget>>,
) -> (Arc<[ScrapeTarget]>, usize) {
    match internal {
        Ok(internal) if internal.is_empty() => (existing, 0),
        Ok(internal) => {
            let mut combined = existing.to_vec();
            combined.extend(internal);
            (combined.into(), 0)
        }
        Err(error) => {
            tracing::warn!(%error, "SRE metrics discovery unavailable; continuing unrelated scrapes");
            (existing, 1)
        }
    }
}

pub(super) fn is_internal(target: &ScrapeTarget) -> bool {
    target.identity.source == "internal"
        && (target.identity.key == TARGET_KEY
            || target.identity.key.starts_with(&format!("{TARGET_KEY}/")))
}

pub(super) fn target(config: &Config) -> Result<Option<ScrapeTarget>> {
    let service = match (&config.sre_metrics_service, &config.sre_metrics_namespace) {
        (None, None) => None,
        (Some(service), Some(namespace))
            if valid_dns_label(service) && valid_dns_label(namespace) =>
        {
            Some((service, namespace))
        }
        _ => bail!("SRE metrics Service and namespace must be configured together as DNS labels"),
    };
    let (url, _file) = match (&config.sre_metrics_url, &config.sre_metrics_token_file) {
        (None, None) if service.is_none() => return Ok(None),
        (Some(url), Some(file)) if !file.as_os_str().is_empty() => (url, file),
        _ => bail!("SRE metrics URL and token file must be configured together"),
    };
    let url = validate_scrape_url(url, &config.scrape_allowed_destinations)?;
    if url.path() != "/metrics" || url.query().is_some() {
        bail!("SRE metrics endpoint must use /metrics without query parameters");
    }
    if let Some((service, namespace)) = service {
        let host = url.host_str().unwrap_or_default();
        let qualified = format!("{service}.{namespace}.svc");
        if host != service
            && host != format!("{service}.{namespace}")
            && host != qualified
            && !host.starts_with(&format!("{qualified}."))
        {
            bail!("SRE metrics URL must address the configured Kubernetes Service");
        }
    }
    Ok(Some(ScrapeTarget {
        labels: vec![
            ("job".into(), "sre-agent".into()),
            ("service".into(), "sre-agent".into()),
            (
                "instance".into(),
                format!(
                    "{}:{}",
                    url.host_str().unwrap_or_default(),
                    url.port_or_known_default().unwrap_or(0)
                ),
            ),
        ],
        url: url.to_string(),
        identity: TargetIdentity {
            key: TARGET_KEY.into(),
            pair: "internal".into(),
            source: "internal".into(),
            namespace: String::new(),
            name: "sre-agent".into(),
        },
    }))
}

fn valid_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value.as_bytes()[value.len() - 1].is_ascii_alphanumeric()
}

/// Resolve only the explicitly configured Service. CRDs cannot select a token-bearing target.
pub(super) async fn targets(config: &Config, client: kube::Client) -> Result<Vec<ScrapeTarget>> {
    let Some(base) = target(config)? else {
        return Ok(Vec::new());
    };
    let (Some(service), Some(namespace)) =
        (&config.sre_metrics_service, &config.sre_metrics_namespace)
    else {
        return Ok(vec![base]);
    };
    let services: Api<Service> = Api::namespaced(client.clone(), namespace);
    let service = services
        .get(service)
        .await
        .context("get SRE metrics Service")?;
    let spec = service
        .spec
        .as_ref()
        .context("SRE metrics Service lacks a spec")?;
    let selector = spec
        .selector
        .as_ref()
        .filter(|labels| !labels.is_empty())
        .context("SRE metrics Service needs a Pod selector")?;
    let label_query = selector
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    let slices: Api<EndpointSlice> = Api::namespaced(client.clone(), namespace);
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    let slice_params = ListParams::default()
        .labels(&format!(
            "kubernetes.io/service-name={}",
            service
                .metadata
                .name
                .as_deref()
                .context("SRE Service lacks a name")?
        ))
        .limit(MAX_ENDPOINTS as u32);
    let pod_params = ListParams::default()
        .labels(&label_query)
        .limit(MAX_ENDPOINTS as u32);
    let (slices, pods) = tokio::try_join!(slices.list(&slice_params), pods.list(&pod_params))
        .context("list SRE metrics EndpointSlices and Pods")?;
    if slices
        .metadata
        .continue_
        .is_some_and(|token| !token.is_empty())
        || pods
            .metadata
            .continue_
            .is_some_and(|token| !token.is_empty())
        || slices.items.len() > MAX_ENDPOINTS
        || pods.items.len() > MAX_ENDPOINTS
    {
        bail!("SRE metrics discovery exceeds the endpoint limit");
    }
    verified_targets(config, &base, &service, &slices.items, &pods.items)
}

fn verified_targets(
    config: &Config,
    base: &ScrapeTarget,
    service: &Service,
    slices: &[EndpointSlice],
    pods: &[Pod],
) -> Result<Vec<ScrapeTarget>> {
    let service_name = config
        .sre_metrics_service
        .as_deref()
        .context("SRE Service is missing")?;
    let namespace = config
        .sre_metrics_namespace
        .as_deref()
        .context("SRE namespace is missing")?;
    if service.metadata.name.as_deref() != Some(service_name)
        || service.metadata.namespace.as_deref() != Some(namespace)
    {
        bail!("SRE metrics Service identity does not match configuration");
    }
    let service_uid = service
        .metadata
        .uid
        .as_deref()
        .context("SRE metrics Service lacks a UID")?;
    let base_url = Url::parse(&base.url)?;
    let service_spec = service
        .spec
        .as_ref()
        .context("SRE metrics Service lacks a spec")?;
    let selector = service_spec
        .selector
        .as_ref()
        .filter(|labels| !labels.is_empty())
        .context("SRE metrics Service needs a Pod selector")?;
    let port = service_spec
        .ports
        .iter()
        .flatten()
        .find(|port| {
            port.name.as_deref() == Some("http")
                && port.protocol.as_deref().unwrap_or("TCP") == "TCP"
                && Some(port.port) == base_url.port_or_known_default().map(i32::from)
        })
        .context("SRE metrics URL must match the Service HTTP port")?;
    let pods: std::collections::HashMap<_, _> = pods
        .iter()
        .filter_map(|pod| {
            let name = pod.metadata.name.as_deref()?;
            let pod_spec = pod.spec.as_ref()?;
            let status = pod.status.as_ref()?;
            let labels = pod.metadata.labels.as_ref()?;
            (pod.metadata.namespace.as_deref() == Some(namespace)
                && pod.metadata.deletion_timestamp.is_none()
                && !pod_spec.host_network.unwrap_or(false)
                && selector
                    .iter()
                    .all(|(key, value)| labels.get(key) == Some(value))
                && status
                    .conditions
                    .iter()
                    .flatten()
                    .any(|condition| condition.type_ == "Ready" && condition.status == "True"))
            .then_some((name, pod))
        })
        .collect();
    let mut found = HashSet::new();
    let mut targets = Vec::new();
    for slice in slices {
        let Some(labels) = slice.metadata.labels.as_ref() else {
            continue;
        };
        if slice.metadata.namespace.as_deref() != Some(namespace)
            || labels.get("kubernetes.io/service-name").map(String::as_str) != Some(service_name)
            || labels
                .get("endpointslice.kubernetes.io/managed-by")
                .map(String::as_str)
                != Some("endpointslice-controller.k8s.io")
            || !slice
                .metadata
                .owner_references
                .iter()
                .flatten()
                .any(|owner| {
                    owner.api_version == "v1"
                        && owner.kind == "Service"
                        && owner.name == service_name
                        && owner.uid == service_uid
                        && owner.controller == Some(true)
                })
        {
            continue;
        }
        let Some(slice_port) = slice
            .ports
            .iter()
            .flatten()
            .find(|port| {
                port.name.as_deref() == Some("http")
                    && port.protocol.as_deref().unwrap_or("TCP") == "TCP"
            })
            .and_then(|port| port.port)
            .and_then(|port| u16::try_from(port).ok())
            .filter(|port| *port != 0)
        else {
            continue;
        };
        for endpoint in &slice.endpoints {
            if !endpoint.conditions.as_ref().is_some_and(|condition| {
                condition.ready == Some(true) && condition.terminating != Some(true)
            }) {
                continue;
            }
            let Some(reference) = endpoint.target_ref.as_ref() else {
                continue;
            };
            let Some(pod) = reference.name.as_deref().and_then(|name| pods.get(name)) else {
                continue;
            };
            if reference.kind.as_deref() != Some("Pod")
                || reference.namespace.as_deref() != Some(namespace)
                || reference.uid.is_none()
                || reference.uid != pod.metadata.uid
                || pod_http_port(pod, port) != Some(i32::from(slice_port))
            {
                continue;
            }
            let Some(status) = pod.status.as_ref() else {
                continue;
            };
            for address in &endpoint.addresses {
                if status.pod_ip.as_ref() != Some(address)
                    && !status.pod_ips.iter().flatten().any(|ip| ip.ip == *address)
                {
                    continue;
                }
                let Ok(ip) = address.parse::<std::net::IpAddr>() else {
                    continue;
                };
                let instance = std::net::SocketAddr::new(ip, slice_port).to_string();
                if !found.insert(instance.clone()) {
                    continue;
                }
                if targets.len() >= MAX_ENDPOINTS {
                    bail!("SRE metrics Service exceeds endpoint limit");
                }
                let url = format!("{}://{instance}/metrics", base_url.scheme());
                validate_scrape_url(&url, &config.scrape_allowed_destinations)?;
                targets.push(ScrapeTarget {
                    url,
                    labels: vec![
                        ("job".into(), "sre-agent".into()),
                        ("service".into(), service_name.into()),
                        ("instance".into(), instance.clone()),
                    ],
                    identity: TargetIdentity {
                        key: format!("{TARGET_KEY}/{instance}"),
                        pair: "internal".into(),
                        source: "internal".into(),
                        namespace: namespace.into(),
                        name: service_name.into(),
                    },
                });
            }
        }
    }
    if targets.is_empty() {
        bail!("SRE metrics Service has no verified ready Pod endpoints");
    }
    Ok(targets)
}

fn pod_http_port(pod: &Pod, service_port: &ServicePort) -> Option<i32> {
    match service_port.target_port.as_ref() {
        None => Some(service_port.port),
        Some(IntOrString::Int(port)) => Some(*port),
        Some(IntOrString::String(name)) => pod
            .spec
            .as_ref()?
            .containers
            .iter()
            .flat_map(|container| container.ports.iter().flatten())
            .find(|port| {
                port.name.as_ref() == Some(name)
                    && port.protocol.as_deref().unwrap_or("TCP") == "TCP"
            })
            .map(|port| port.container_port),
    }
}

pub(super) async fn request(
    client: &Client,
    scrape_target: &ScrapeTarget,
    url: Url,
    config: &Config,
) -> Result<RequestBuilder> {
    let request = client.get(url.clone());
    if !is_internal(scrape_target) {
        return Ok(request);
    }
    let configured = target(config)?.context("internal SRE metrics target is not configured")?;
    let endpoint_allowed = if let (Some(service), Some(namespace)) =
        (&config.sre_metrics_service, &config.sre_metrics_namespace)
    {
        scrape_target.identity.name == *service
            && scrape_target.identity.namespace == *namespace
            && scrape_target.url == url.as_str()
            && Url::parse(&configured.url)?.scheme() == url.scheme()
            && url.path() == "/metrics"
            && url.query().is_none()
    } else {
        configured.url == url.as_str()
    };
    if !endpoint_allowed {
        bail!("internal SRE metrics target does not match configured endpoint");
    }
    let file = config
        .sre_metrics_token_file
        .as_ref()
        .context("SRE token file is missing")?;
    let mut bytes = Vec::new();
    tokio::fs::File::open(file)
        .await
        .context("open SRE metrics token file")?
        .take(MAX_TOKEN_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .context("read SRE metrics token file")?;
    if bytes.len() as u64 > MAX_TOKEN_BYTES {
        bail!("SRE metrics token exceeds size limit");
    }
    let token = std::str::from_utf8(&bytes)
        .context("SRE metrics token is not UTF-8")?
        .trim();
    if token.is_empty() {
        bail!("SRE metrics token is empty");
    }
    let mut header = HeaderValue::from_str(token).context("invalid SRE metrics token header")?;
    header.set_sensitive(true);
    Ok(request.header("x-rush-internal-token", header))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::path::PathBuf;

    struct TokenFile(PathBuf);
    impl TokenFile {
        fn new() -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            Self(std::env::temp_dir().join(format!(
                "metrics-agent-test-token-{}-{unique}",
                std::process::id()
            )))
        }
        fn config(&self, url: &str) -> Config {
            Config::try_parse_from([
                "metrics-agent",
                "--sre-metrics-url",
                url,
                "--sre-metrics-token-file",
                self.0.to_str().unwrap(),
                "--scrape-allowed-destinations",
                "127.0.0.1",
            ])
            .unwrap()
        }
    }
    impl Drop for TokenFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn internal_target_requires_complete_explicit_configuration() {
        let file = TokenFile::new();
        let mut config = file.config("http://sre-agent:8081/metrics");
        assert!(target(&config).unwrap().is_some());
        config.sre_metrics_token_file = None;
        assert!(target(&config).is_err());
        config.sre_metrics_url = None;
        assert!(target(&config).unwrap().is_none());
        config.sre_metrics_token_file = Some(file.0.clone());
        assert!(target(&config).is_err());
        for url in [
            "http://sre-agent:8081/healthz",
            "http://sre-agent:8081/metrics?token=value",
            "http://user:value@sre-agent:8081/metrics",
            "http://169.254.169.254/metrics",
        ] {
            assert!(target(&file.config(url)).is_err());
        }
    }

    #[tokio::test]
    async fn credentials_rotate_and_never_reach_crd_targets() {
        let file = TokenFile::new();
        let config = file.config("http://127.0.0.1:8081/metrics");
        let internal = target(&config).unwrap().unwrap();
        let client = Client::new();
        let url = Url::parse(&internal.url).unwrap();
        for token in ["first-test-token", "rotated-test-token\n"] {
            tokio::fs::write(&file.0, token).await.unwrap();
            let built = request(&client, &internal, url.clone(), &config)
                .await
                .unwrap()
                .build()
                .unwrap();
            let header = &built.headers()["x-rush-internal-token"];
            assert_eq!(header, token.trim());
            assert!(header.is_sensitive());
            assert!(!format!("{:?}", built.headers()).contains(token.trim()));
        }
        let mut crd = internal.clone();
        crd.identity.source = "prometheus".into();
        let built = request(&client, &crd, url.clone(), &config)
            .await
            .unwrap()
            .build()
            .unwrap();
        assert!(!built.headers().contains_key("x-rush-internal-token"));
        let wrong = Url::parse("http://127.0.0.1:8082/metrics").unwrap();
        assert!(request(&client, &internal, wrong, &config).await.is_err());
        for token in [
            vec![],
            vec![b'x'; 4097],
            b"line\nbreak".to_vec(),
            vec![0xff],
        ] {
            tokio::fs::write(&file.0, token).await.unwrap();
            assert!(
                request(&client, &internal, url.clone(), &config)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn authenticated_scrape_reaches_protected_endpoint() {
        use axum::{Router, extract::Request, http::StatusCode, routing::get};
        let server = Router::new().route(
            "/metrics",
            get(|request: Request| async move {
                if request
                    .headers()
                    .get("x-rush-internal-token")
                    .is_some_and(|value| value == "local-test-token")
                {
                    (StatusCode::OK, "sre_agent_readiness 1\n")
                } else {
                    (StatusCode::UNAUTHORIZED, "")
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });
        let file = TokenFile::new();
        tokio::fs::write(&file.0, "local-test-token").await.unwrap();
        let config = file.config(&format!("http://{address}/metrics"));
        let internal = target(&config).unwrap().unwrap();
        let client = super::super::scrape_client(
            std::time::Duration::from_secs(2),
            config.scrape_allowed_destinations.clone(),
        )
        .unwrap();
        let response = request(
            &client,
            &internal,
            Url::parse(&internal.url).unwrap(),
            &config,
        )
        .await
        .unwrap()
        .send()
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "sre_agent_readiness 1\n");
        task.abort();
    }

    fn service_config(file: &TokenFile) -> Config {
        let mut config = file.config("http://sre-agent.health.svc:8081/metrics");
        config.sre_metrics_service = Some("sre-agent".into());
        config.sre_metrics_namespace = Some("health".into());
        config
    }

    fn service() -> Service {
        serde_json::from_value(serde_json::json!({
            "apiVersion":"v1", "kind":"Service",
            "metadata":{"name":"sre-agent", "namespace":"health", "uid":"service-uid"},
            "spec":{"selector":{"component":"sre-agent"}, "ports":[{"name":"http","port":8081,"targetPort":"http"}]}
        })).unwrap()
    }

    fn fixtures(ports: &[u16]) -> (Vec<EndpointSlice>, Vec<Pod>) {
        ports.iter().enumerate().map(|(index, port)| {
            let name = format!("sre-{index}");
            let uid = format!("pod-{index}");
            let slice = serde_json::from_value(serde_json::json!({
                "apiVersion":"discovery.k8s.io/v1", "kind":"EndpointSlice",
                "metadata":{"name": name, "namespace":"health",
                    "labels":{"kubernetes.io/service-name":"sre-agent",
                              "endpointslice.kubernetes.io/managed-by":"endpointslice-controller.k8s.io"},
                    "ownerReferences":[{"apiVersion":"v1","kind":"Service","name":"sre-agent","uid":"service-uid","controller":true}]},
                "addressType":"IPv4", "ports":[{"name":"http","port":port}],
                "endpoints":[{"addresses":["127.0.0.1"], "conditions":{"ready":true},
                    "targetRef":{"kind":"Pod","namespace":"health","name":name,"uid":uid}}]
            })).unwrap();
            let pod = serde_json::from_value(serde_json::json!({
                "apiVersion":"v1","kind":"Pod",
                "metadata":{"name":name,"namespace":"health","uid":uid,"labels":{"component":"sre-agent"}},
                "spec":{"containers":[{"name":"sre-agent","ports":[{"name":"http","containerPort":port}]}]},
                "status":{"podIP":"127.0.0.1","conditions":[{"type":"Ready","status":"True"}]}
            })).unwrap();
            (slice, pod)
        }).unzip()
    }

    #[test]
    fn service_fanout_is_verified_and_deduplicated() {
        let file = TokenFile::new();
        let config = service_config(&file);
        let base = target(&config).unwrap().unwrap();
        let (mut slices, mut pods) = fixtures(&[8081, 8082, 8081]);
        let selected = verified_targets(&config, &base, &service(), &slices, &pods).unwrap();
        assert_eq!(selected.len(), 2);
        assert!(selected.iter().all(is_internal));
        slices[0].metadata.owner_references.as_mut().unwrap()[0].uid = "old-service".into();
        slices[1].endpoints[0].addresses[0] = "10.2.0.1".into();
        pods[2].spec.as_mut().unwrap().host_network = Some(true);
        assert!(verified_targets(&config, &base, &service(), &slices, &pods).is_err());
    }

    #[test]
    fn unavailable_internal_source_preserves_other_workloads_and_counts_degradation() {
        let file = TokenFile::new();
        let base = target(&file.config("http://sre-agent:8081/metrics"))
            .unwrap()
            .unwrap();
        let existing: Arc<[ScrapeTarget]> = vec![base].into();
        let (kept, failures) =
            combine(existing.clone(), Err(anyhow::anyhow!("no ready endpoints")));
        assert!(Arc::ptr_eq(&kept, &existing));
        assert_eq!(kept.len() + failures, 2);
        assert_eq!(failures, 1);
        let (kept, failures) = combine(existing.clone(), Ok(Vec::new()));
        assert!(Arc::ptr_eq(&kept, &existing));
        assert_eq!(failures, 0);
    }

    #[tokio::test]
    async fn kubernetes_service_replicas_publish_distinct_instances() {
        use axum::{Json, Router, extract::Request, http::StatusCode, routing::get};
        let file = TokenFile::new();
        tokio::fs::write(&file.0, "local-test-token").await.unwrap();
        let mut tasks = Vec::new();
        let mut ports = Vec::new();
        for value in [11, 22] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            ports.push(listener.local_addr().unwrap().port());
            let app = Router::new().route(
                "/metrics",
                get(move |request: Request| async move {
                    if request
                        .headers()
                        .get("x-rush-internal-token")
                        .is_some_and(|v| v == "local-test-token")
                    {
                        (
                            StatusCode::OK,
                            format!("sre_agent_investigations_completed_total {value}\n"),
                        )
                    } else {
                        (StatusCode::UNAUTHORIZED, String::new())
                    }
                }),
            );
            tasks.push(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap()
            }));
        }
        let (slices, pods) = fixtures(&ports);
        let api = Router::new()
            .route(
                "/api/v1/namespaces/health/services/sre-agent",
                get(|| async { Json(service()) }),
            )
            .route(
                "/apis/discovery.k8s.io/v1/namespaces/health/endpointslices",
                get(move || {
                    let slices = slices.clone();
                    async move { Json(serde_json::json!({"metadata":{}, "items":slices})) }
                }),
            )
            .route(
                "/api/v1/namespaces/health/pods",
                get(move || {
                    let pods = pods.clone();
                    async move { Json(serde_json::json!({"metadata":{}, "items":pods})) }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tasks.push(tokio::spawn(async move {
            axum::serve(listener, api).await.unwrap()
        }));
        let kube = kube::Client::try_from(kube::Config::new(
            format!("http://{address}").parse().unwrap(),
        ))
        .unwrap();
        let config = service_config(&file);
        let replicas = targets(&config, kube).await.unwrap();
        assert_eq!(replicas.len(), 2);
        let client = super::super::scrape_client(
            std::time::Duration::from_secs(2),
            config.scrape_allowed_destinations.clone(),
        )
        .unwrap();
        let mut results = Vec::new();
        for replica in &replicas {
            let response = request(&client, replica, Url::parse(&replica.url).unwrap(), &config)
                .await
                .unwrap()
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            results.push((replica.labels[2].1.clone(), response.text().await.unwrap()));
        }
        assert_ne!(results[0].0, results[1].0);
        assert!(results[0].1.ends_with("11\n"));
        assert!(results[1].1.ends_with("22\n"));
        for task in tasks {
            task.abort();
        }
    }

    #[tokio::test]
    async fn internal_discovery_failure_does_not_block_workload_delivery() {
        use super::super::{TargetCache, scrape_once};
        use crate::controller::Controller;
        use axum::{Json, Router, http::StatusCode, routing::get};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/workload", get(|| async { "workload_ok 1\n" }))
            .route("/api/v1/namespaces/health/services/sre-agent", get(|| async {
                (StatusCode::FORBIDDEN, Json(serde_json::json!({"apiVersion":"v1","kind":"Status","status":"Failure","reason":"Forbidden","message":"forbidden","code":403})))
            }));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let file = TokenFile::new();
        let config = service_config(&file);
        let client = kube::Client::try_from(kube::Config::new(
            format!("http://{address}").parse().unwrap(),
        ))
        .unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
        let controller = Controller::new(client, config.clone(), sender.clone());
        let cached = ScrapeTarget {
            url: format!("http://{address}/workload"),
            labels: Vec::new(),
            identity: TargetIdentity {
                key: "workload".into(),
                pair: "service-monitor".into(),
                source: "prometheus".into(),
                namespace: "health".into(),
                name: "workload".into(),
            },
        };
        let mut cache = TargetCache {
            refreshed_at: Some(std::time::Instant::now()),
            targets: vec![cached].into(),
        };
        let http = super::super::scrape_client(
            std::time::Duration::from_secs(2),
            config.scrape_allowed_destinations.clone(),
        )
        .unwrap();
        let (targets, healthy, samples, _, _) =
            scrape_once(&controller, &http, &sender, &mut cache)
                .await
                .unwrap();
        assert_eq!((targets, healthy, samples), (2, 1, 1));
        let metrics = crate::scrape_metrics::registry().render();
        assert!(metrics.contains("metrics_agent_scrape_cycles_total{outcome=\"partial\"}"));
        assert!(metrics.contains("metrics_agent_internal_discovery_failures_total"));
        assert!(matches!(
            receiver.recv().await,
            Some(crate::remote_write::ScrapeMessage::Start { targets: 2 })
        ));
        match receiver.recv().await {
            Some(crate::remote_write::ScrapeMessage::Batch(batch)) => {
                assert_eq!(batch[0].name, "workload_ok")
            }
            _ => panic!("workload sample must still be delivered"),
        }
        assert!(matches!(
            receiver.recv().await,
            Some(crate::remote_write::ScrapeMessage::Complete {
                targets: 2,
                healthy_targets: 1,
                ..
            })
        ));

        // An unresolved internal source with no other workloads is failed, not a
        // successful empty cycle, and still produces no phantom HTTP request.
        cache.targets = Vec::new().into();
        let (targets, healthy, samples, _, _) =
            scrape_once(&controller, &http, &sender, &mut cache)
                .await
                .unwrap();
        assert_eq!((targets, healthy, samples), (1, 0, 0));
        assert!(
            crate::scrape_metrics::registry()
                .render()
                .contains("metrics_agent_scrape_cycles_total{outcome=\"failed\"}")
        );
        task.abort();
    }
}
