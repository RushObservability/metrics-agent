//! Credentials for this explicit operator-configured target never reach CRD targets.
use anyhow::{Context, Result, bail};
use k8s_openapi::api::core::v1::Endpoints;
use kube::Api;
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
    let api: Api<Endpoints> = Api::namespaced(client, namespace);
    let endpoints = api
        .get(service)
        .await
        .context("discover SRE metrics Service endpoints")?;
    from_endpoints(config, &base, &endpoints)
}

fn from_endpoints(
    config: &Config,
    base: &ScrapeTarget,
    endpoints: &Endpoints,
) -> Result<Vec<ScrapeTarget>> {
    let service = config
        .sre_metrics_service
        .as_ref()
        .context("SRE Service is missing")?;
    let namespace = config
        .sre_metrics_namespace
        .as_ref()
        .context("SRE namespace is missing")?;
    if endpoints.metadata.name.as_ref() != Some(service)
        || endpoints.metadata.namespace.as_ref() != Some(namespace)
    {
        bail!("SRE endpoint identity does not match the configured Service");
    }
    let base_url = Url::parse(&base.url)?;
    let mut found = HashSet::new();
    let mut targets = Vec::new();
    for subset in endpoints.subsets.iter().flatten() {
        let Some(port) = subset
            .ports
            .iter()
            .flatten()
            .find(|port| {
                port.name.as_deref() == Some("http")
                    && port.protocol.as_deref().unwrap_or("TCP") == "TCP"
            })
            .map(|port| port.port)
        else {
            continue;
        };
        let port = u16::try_from(port)
            .ok()
            .filter(|port| *port != 0)
            .context("SRE metrics endpoint port is invalid")?;
        for address in subset.addresses.iter().flatten() {
            let ip = address
                .ip
                .parse::<std::net::IpAddr>()
                .context("SRE endpoint address is not an IP")?;
            let instance = std::net::SocketAddr::new(ip, port).to_string();
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
                    ("service".into(), service.clone()),
                    ("instance".into(), instance.clone()),
                ],
                identity: TargetIdentity {
                    key: format!("{TARGET_KEY}/{instance}"),
                    pair: "internal".into(),
                    source: "internal".into(),
                    namespace: namespace.clone(),
                    name: service.clone(),
                },
            });
        }
    }
    if targets.is_empty() {
        bail!("SRE metrics Service has no ready HTTP endpoints");
    }
    Ok(targets)
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

    fn endpoints(ports: &[u16]) -> Endpoints {
        serde_json::from_value(serde_json::json!({
            "metadata": {"name":"sre-agent","namespace":"health"},
            "subsets": ports.iter().map(|port| serde_json::json!({
                "addresses":[{"ip":"127.0.0.1"}],
                "notReadyAddresses":[{"ip":"127.0.0.2"}],
                "ports":[{"name":"http","protocol":"TCP","port":port}]
            })).collect::<Vec<_>>()
        }))
        .unwrap()
    }

    #[test]
    fn service_fanout_is_ready_only_deduplicated_bounded_and_scoped() {
        let file = TokenFile::new();
        let config = service_config(&file);
        let base = target(&config).unwrap().unwrap();
        let selected = from_endpoints(&config, &base, &endpoints(&[8081, 8082, 8081])).unwrap();
        assert_eq!(selected.len(), 2);
        assert_ne!(selected[0].identity.key, selected[1].identity.key);
        assert_eq!(selected[0].labels[2].1, "127.0.0.1:8081");
        assert_eq!(selected[1].labels[2].1, "127.0.0.1:8082");
        assert!(selected.iter().all(is_internal));
        assert!(from_endpoints(&config, &base, &endpoints(&[])).is_err());
        let mut wrong = endpoints(&[8081]);
        wrong.metadata.namespace = Some("other".into());
        assert!(from_endpoints(&config, &base, &wrong).is_err());
        let excessive = (1024..1281).collect::<Vec<_>>();
        assert!(from_endpoints(&config, &base, &endpoints(&excessive)).is_err());
        let mut partial = service_config(&file);
        partial.sre_metrics_namespace = None;
        assert!(target(&partial).is_err());
        let mut external = service_config(&file);
        external.sre_metrics_url = Some("http://other-service/metrics".into());
        assert!(target(&external).is_err());
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
        let data = endpoints(&ports);
        let api = Router::new().route(
            "/api/v1/namespaces/health/endpoints/sre-agent",
            get(move || {
                let data = data.clone();
                async move { Json(data) }
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
            .route("/api/v1/namespaces/health/endpoints/sre-agent", get(|| async {
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
        task.abort();
    }
}
