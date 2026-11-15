//! Credentials for this explicit operator-configured target never reach CRD targets.
use anyhow::{Context, Result, bail};
use reqwest::{Client, RequestBuilder, Url, header::HeaderValue};
use tokio::io::AsyncReadExt;

use super::{ScrapeTarget, TargetIdentity, validate_scrape_url};
use crate::config::Config;

const TARGET_KEY: &str = "internal/sre-agent";
const MAX_TOKEN_BYTES: u64 = 4096;

pub(super) fn is_internal(target: &ScrapeTarget) -> bool {
    target.identity.source == "internal" && target.identity.key == TARGET_KEY
}

pub(super) fn target(config: &Config) -> Result<Option<ScrapeTarget>> {
    let (url, _file) = match (&config.sre_metrics_url, &config.sre_metrics_token_file) {
        (None, None) => return Ok(None),
        (Some(url), Some(file)) if !file.as_os_str().is_empty() => (url, file),
        _ => bail!("SRE metrics URL and token file must be configured together"),
    };
    let url = validate_scrape_url(url, &config.scrape_allowed_destinations)?;
    if url.path() != "/metrics" || url.query().is_some() {
        bail!("SRE metrics endpoint must use /metrics without query parameters");
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
    if configured.url != url.as_str() {
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
}
