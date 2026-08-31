use std::{
    collections::BTreeMap, env, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;

fn parse_duration(value: &str) -> Result<Duration, String> {
    humantime::parse_duration(value).map_err(|error| error.to_string())
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|error| format!("invalid positive integer: {error}"))?;
    if value == 0 {
        return Err("value must be greater than zero".to_string());
    }
    Ok(value)
}

fn parse_extra_labels(value: &str) -> Result<BTreeMap<String, String>, String> {
    let labels: BTreeMap<String, String> =
        serde_json::from_str(value).map_err(|error| format!("invalid labels JSON: {error}"))?;
    for name in labels.keys() {
        if name == "__name__" {
            return Err("extra label __name__ is reserved by Prometheus".to_string());
        }
        let mut chars = name.chars();
        if !chars
            .next()
            .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
            || !chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
        {
            return Err(format!("invalid Prometheus label name {name:?}"));
        }
    }
    Ok(labels)
}

#[derive(Clone, Debug, Parser)]
#[command(
    name = "metrics-agent",
    version,
    about = "Kubernetes metrics scrape controller"
)]
pub struct Config {
    #[arg(long, env = "METRICS_AGENT_HTTP_ADDRESS", default_value = ":7070")]
    pub http_address: String,

    #[arg(long, env = "METRICS_AGENT_KUBECONFIG")]
    pub kubeconfig: Option<PathBuf>,

    #[arg(
        long,
        env = "METRICS_AGENT_RESYNC_PERIOD",
        value_parser = parse_duration,
        default_value = "5m"
    )]
    pub resync_period: Duration,

    #[arg(long, env = "METRICS_AGENT_WORKERS", default_value_t = 2)]
    pub workers: usize,

    #[arg(long, env = "METRICS_AGENT_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    #[arg(long, env = "METRICS_AGENT_UI_ENABLED", default_value_t = false)]
    pub ui_enabled: bool,

    #[arg(long, env = "METRICS_AGENT_UI_ADDRESS", default_value = ":7070")]
    pub ui_address: String,

    #[arg(long, env = "METRICS_AGENT_UI_PATH", default_value = "/ui/")]
    pub ui_path: String,

    #[arg(long, env = "RUSH_REMOTE_WRITE_URL")]
    pub rush_remote_write_url: Option<String>,

    #[arg(
        long,
        env = "RUSH_REMOTE_WRITE_INTERVAL",
        value_parser = parse_duration,
        default_value = "15s"
    )]
    pub rush_remote_write_interval: Duration,

    #[arg(
        long,
        env = "RUSH_REMOTE_WRITE_TIMEOUT",
        value_parser = parse_duration,
        default_value = "30s"
    )]
    pub rush_remote_write_timeout: Duration,

    #[arg(
        long,
        env = "RUSH_REMOTE_WRITE_CONNECT_TIMEOUT",
        value_parser = parse_duration,
        default_value = "5s"
    )]
    pub rush_remote_write_connect_timeout: Duration,

    #[arg(long, env = "RUSH_REMOTE_WRITE_TOKEN")]
    pub rush_remote_write_token: Option<String>,

    #[arg(long, env = "RUSH_REMOTE_WRITE_TENANT")]
    pub rush_remote_write_tenant: Option<String>,

    #[arg(
        long,
        env = "METRICS_AGENT_EXTRA_LABELS",
        value_parser = parse_extra_labels,
        default_value = "{}"
    )]
    pub extra_labels: BTreeMap<String, String>,

    #[arg(long, env = "METRICS_AGENT_SCRAPE_ENABLED", default_value_t = true)]
    pub scrape_enabled: bool,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_INTERVAL",
        value_parser = parse_duration,
        default_value = "15s"
    )]
    pub scrape_interval: Duration,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_TIMEOUT",
        value_parser = parse_duration,
        default_value = "10s"
    )]
    pub scrape_timeout: Duration,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_DISCOVERY_REFRESH_INTERVAL",
        value_parser = parse_duration,
        default_value = "60s"
    )]
    pub scrape_discovery_refresh_interval: Duration,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_CONCURRENCY",
        value_parser = parse_positive_usize,
        default_value_t = 8
    )]
    pub scrape_concurrency: usize,

    /// Source namespaces whose scrape objects may create targets. Empty means
    /// all namespaces visible to the controller.
    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_ALLOWED_NAMESPACES",
        value_delimiter = ','
    )]
    pub scrape_allowed_namespaces: Vec<String>,

    /// Exact hosts, IP addresses, or `*.suffix` patterns that may override the
    /// built-in destination denylist. This is intentionally empty by default.
    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_ALLOWED_DESTINATIONS",
        value_delimiter = ','
    )]
    pub scrape_allowed_destinations: Vec<String>,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_MAX_RESPONSE_BYTES",
        value_parser = parse_positive_usize,
        default_value_t = 4_194_304
    )]
    pub scrape_max_response_bytes: usize,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_MAX_SAMPLES_PER_TARGET",
        value_parser = parse_positive_usize,
        default_value_t = 50_000
    )]
    pub scrape_max_samples_per_target: usize,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_MAX_LABELS_PER_SAMPLE",
        value_parser = parse_positive_usize,
        default_value_t = 64
    )]
    pub scrape_max_labels_per_sample: usize,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_MAX_LABEL_NAME_BYTES",
        value_parser = parse_positive_usize,
        default_value_t = 256
    )]
    pub scrape_max_label_name_bytes: usize,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_MAX_LABEL_VALUE_BYTES",
        value_parser = parse_positive_usize,
        default_value_t = 4_096
    )]
    pub scrape_max_label_value_bytes: usize,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_MAX_METRIC_NAME_BYTES",
        value_parser = parse_positive_usize,
        default_value_t = 1_024
    )]
    pub scrape_max_metric_name_bytes: usize,

    #[arg(
        long,
        env = "METRICS_AGENT_SCRAPE_MAX_LINE_BYTES",
        value_parser = parse_positive_usize,
        default_value_t = 65_536
    )]
    pub scrape_max_line_bytes: usize,

    #[arg(long, env = "METRICS_AGENT_VERSION", default_value = "dev")]
    pub agent_version: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self::parse()
    }

    pub fn http_socket_addr(&self) -> Result<SocketAddr> {
        parse_listen_address(&self.http_address)
    }

    pub fn ui_socket_addr(&self) -> Result<SocketAddr> {
        parse_listen_address(&self.ui_address)
    }

    pub async fn kube_config(&self) -> Result<kube::Config> {
        if let Some(path) = self.kubeconfig.as_ref() {
            let kubeconfig = kube::config::Kubeconfig::read_from(path)
                .with_context(|| format!("read kubeconfig {}", path.display()))?;
            return kube::Config::from_custom_kubeconfig(
                kubeconfig,
                &kube::config::KubeConfigOptions::default(),
            )
            .await
            .context("build Kubernetes client configuration");
        }

        kube::Config::infer()
            .await
            .context("infer in-cluster or local Kubernetes configuration")
    }
}

fn parse_listen_address(value: &str) -> Result<SocketAddr> {
    if let Ok(address) = value.parse() {
        return Ok(address);
    }

    let (host, port) = value
        .rsplit_once(':')
        .with_context(|| format!("listen address must be :PORT or HOST:PORT, got {value:?}"))?;
    let port = u16::from_str(port).with_context(|| format!("invalid port in {value:?}"))?;
    let host = if host.is_empty() { "0.0.0.0" } else { host };
    let host = host
        .parse()
        .with_context(|| format!("invalid listen host in {value:?}"))?;
    Ok(SocketAddr::new(host, port))
}

pub fn configure_logging(level: &str) {
    let filter =
        env::var("RUST_LOG").unwrap_or_else(|_| format!("metrics_agent={level},kube=warn"));
    let _ = tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .try_init();
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        net::SocketAddr,
        path::PathBuf,
        sync::{Mutex, OnceLock},
        time::Duration,
    };

    use clap::Parser;

    use super::{
        Config, parse_duration, parse_extra_labels, parse_listen_address, parse_positive_usize,
    };

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn parses_colon_port_addresses() {
        let _lock = env_lock().lock().unwrap();
        assert_eq!(
            parse_listen_address(":8080").unwrap().to_string(),
            "0.0.0.0:8080"
        );
        assert_eq!(
            parse_listen_address("127.0.0.1:9000").unwrap().to_string(),
            "127.0.0.1:9000"
        );
    }

    #[test]
    fn parses_ipv6_and_rejects_invalid_listen_addresses() {
        let _lock = env_lock().lock().unwrap();
        assert_eq!(
            parse_listen_address("[::1]:7070").unwrap(),
            SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], 7070))
        );
        assert!(parse_listen_address("7070").is_err());
        assert!(parse_listen_address("localhost:7070").is_err());
        assert!(parse_listen_address(":not-a-port").is_err());
        assert!(parse_listen_address(":65536").is_err());
    }

    #[test]
    fn parses_duration_precision_zero_and_invalid_values() {
        let _lock = env_lock().lock().unwrap();
        assert_eq!(parse_duration("1.5s").unwrap(), Duration::from_millis(1500));
        assert_eq!(parse_duration("0s").unwrap(), Duration::ZERO);
        assert!(parse_duration("not-a-duration").is_err());
    }

    #[test]
    fn cli_parsing_covers_overrides_booleans_and_optional_secrets() {
        let _lock = env_lock().lock().unwrap();
        let config = Config::try_parse_from([
            "metrics-agent",
            "--http-address",
            "127.0.0.1:8080",
            "--kubeconfig",
            "/tmp/kubeconfig",
            "--resync-period",
            "1500ms",
            "--workers",
            "4",
            "--log-level",
            "debug",
            "--ui-enabled",
            "--ui-address",
            ":9090",
            "--ui-path",
            "control-room/",
            "--rush-remote-write-url",
            "http://rush.test/write",
            "--rush-remote-write-interval",
            "2m",
            "--rush-remote-write-timeout",
            "12s",
            "--rush-remote-write-connect-timeout",
            "3s",
            "--rush-remote-write-token",
            "secret-token",
            "--rush-remote-write-tenant",
            "tenant-a",
            "--extra-labels",
            r#"{"cluster":"ntt-japan","env":"dev"}"#,
            "--scrape-enabled",
            "--scrape-interval",
            "250ms",
            "--scrape-timeout",
            "1s",
            "--scrape-discovery-refresh-interval",
            "45s",
            "--scrape-concurrency",
            "12",
            "--scrape-allowed-namespaces",
            "monitoring,apps",
            "--scrape-allowed-destinations",
            "metrics.internal,*.trusted.example",
            "--scrape-max-response-bytes",
            "1048576",
            "--scrape-max-samples-per-target",
            "1234",
            "--scrape-max-labels-per-sample",
            "32",
            "--scrape-max-label-name-bytes",
            "128",
            "--scrape-max-label-value-bytes",
            "2048",
            "--scrape-max-metric-name-bytes",
            "512",
            "--scrape-max-line-bytes",
            "32768",
            "--agent-version",
            "1.2.3",
        ])
        .unwrap();

        assert_eq!(config.http_address, "127.0.0.1:8080");
        assert_eq!(config.kubeconfig, Some(PathBuf::from("/tmp/kubeconfig")));
        assert_eq!(config.resync_period, Duration::from_millis(1500));
        assert_eq!(config.workers, 4);
        assert_eq!(config.log_level, "debug");
        assert!(config.ui_enabled);
        assert_eq!(config.ui_address, ":9090");
        assert_eq!(config.ui_path, "control-room/");
        assert_eq!(
            config.rush_remote_write_url.as_deref(),
            Some("http://rush.test/write")
        );
        assert_eq!(config.rush_remote_write_interval, Duration::from_secs(120));
        assert_eq!(config.rush_remote_write_timeout, Duration::from_secs(12));
        assert_eq!(
            config.rush_remote_write_connect_timeout,
            Duration::from_secs(3)
        );
        assert_eq!(
            config.rush_remote_write_token.as_deref(),
            Some("secret-token")
        );
        assert_eq!(config.rush_remote_write_tenant.as_deref(), Some("tenant-a"));
        assert_eq!(
            config.extra_labels.get("env").map(String::as_str),
            Some("dev")
        );
        assert_eq!(
            config.extra_labels.get("cluster").map(String::as_str),
            Some("ntt-japan")
        );
        assert!(config.scrape_enabled);
        assert_eq!(config.scrape_interval, Duration::from_millis(250));
        assert_eq!(config.scrape_timeout, Duration::from_secs(1));
        assert_eq!(
            config.scrape_discovery_refresh_interval,
            Duration::from_secs(45)
        );
        assert_eq!(config.scrape_concurrency, 12);
        assert_eq!(config.scrape_allowed_namespaces, ["monitoring", "apps"]);
        assert_eq!(
            config.scrape_allowed_destinations,
            ["metrics.internal", "*.trusted.example"]
        );
        assert_eq!(config.scrape_max_response_bytes, 1_048_576);
        assert_eq!(config.scrape_max_samples_per_target, 1_234);
        assert_eq!(config.scrape_max_labels_per_sample, 32);
        assert_eq!(config.scrape_max_label_name_bytes, 128);
        assert_eq!(config.scrape_max_label_value_bytes, 2_048);
        assert_eq!(config.scrape_max_metric_name_bytes, 512);
        assert_eq!(config.scrape_max_line_bytes, 32_768);
        assert_eq!(config.agent_version, "1.2.3");
    }

    #[test]
    fn defaults_are_stable_and_invalid_cli_values_fail() {
        let _lock = env_lock().lock().unwrap();
        let config = Config::try_parse_from(["metrics-agent"]).unwrap();
        assert_eq!(config.http_address, ":7070");
        assert_eq!(config.ui_address, ":7070");
        assert_eq!(config.ui_path, "/ui/");
        assert_eq!(config.resync_period, Duration::from_secs(300));
        assert_eq!(config.rush_remote_write_interval, Duration::from_secs(15));
        assert_eq!(config.rush_remote_write_timeout, Duration::from_secs(30));
        assert_eq!(
            config.rush_remote_write_connect_timeout,
            Duration::from_secs(5)
        );
        assert_eq!(config.scrape_interval, Duration::from_secs(15));
        assert_eq!(config.scrape_timeout, Duration::from_secs(10));
        assert_eq!(
            config.scrape_discovery_refresh_interval,
            Duration::from_secs(60)
        );
        assert_eq!(config.scrape_concurrency, 8);
        assert!(config.scrape_allowed_namespaces.is_empty());
        assert!(config.scrape_allowed_destinations.is_empty());
        assert_eq!(config.scrape_max_response_bytes, 4_194_304);
        assert_eq!(config.scrape_max_samples_per_target, 50_000);
        assert_eq!(config.scrape_max_labels_per_sample, 64);
        assert_eq!(config.scrape_max_label_name_bytes, 256);
        assert_eq!(config.scrape_max_label_value_bytes, 4_096);
        assert_eq!(config.scrape_max_metric_name_bytes, 1_024);
        assert_eq!(config.scrape_max_line_bytes, 65_536);
        assert_eq!(config.workers, 2);
        assert!(!config.ui_enabled);
        assert!(config.scrape_enabled);
        assert!(config.extra_labels.is_empty());
        assert!(Config::try_parse_from(["metrics-agent", "--workers", "not-a-number"]).is_err());
        assert!(
            Config::try_parse_from(["metrics-agent", "--scrape-timeout", "not-a-duration"])
                .is_err()
        );
        assert!(Config::try_parse_from(["metrics-agent", "--ui-enabled=maybe"]).is_err());
        assert!(parse_extra_labels(r#"{"__name__":"invalid"}"#).is_err());
        assert!(parse_extra_labels(r#"{"bad-label":"invalid"}"#).is_err());
        assert!(parse_extra_labels(r#"{"env":1}"#).is_err());
        assert!(parse_positive_usize("0").is_err());
        assert!(parse_positive_usize("-1").is_err());
    }

    struct RestoredEnv {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl RestoredEnv {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = env::var_os(key);
            // Environment mutation is process-global; this test only uses unique keys and restores them.
            unsafe { env::set_var(key, value) };
            Self { key, previous }
        }
    }

    impl Drop for RestoredEnv {
        fn drop(&mut self) {
            // Restore the caller's environment even if parsing panics.
            unsafe {
                match &self.previous {
                    Some(value) => env::set_var(self.key, value),
                    None => env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    fn environment_values_are_loaded_with_the_same_parsers_as_cli_values() {
        let _lock = env_lock().lock().unwrap();
        let _ui = RestoredEnv::set("METRICS_AGENT_UI_ENABLED", "false");
        let _scrape = RestoredEnv::set("METRICS_AGENT_SCRAPE_INTERVAL", "275ms");
        let _remote_interval = RestoredEnv::set("RUSH_REMOTE_WRITE_INTERVAL", "2m");
        let _url = RestoredEnv::set("RUSH_REMOTE_WRITE_URL", "http://rush.test/write");
        let _extra_labels = RestoredEnv::set(
            "METRICS_AGENT_EXTRA_LABELS",
            r#"{"cluster":"ntt-japan","env":"dev"}"#,
        );

        let config = Config::try_parse_from(["metrics-agent"]).unwrap();
        assert!(!config.ui_enabled);
        assert_eq!(config.scrape_interval, Duration::from_millis(275));
        assert_eq!(config.rush_remote_write_interval, Duration::from_secs(120));
        assert_eq!(
            config.rush_remote_write_url.as_deref(),
            Some("http://rush.test/write")
        );
        assert_eq!(
            config.extra_labels.get("env").map(String::as_str),
            Some("dev")
        );
    }
}
