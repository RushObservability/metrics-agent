use std::{env, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};

use anyhow::{Context, Result};
use clap::Parser;

fn parse_duration(value: &str) -> Result<Duration, String> {
    humantime::parse_duration(value).map_err(|error| error.to_string())
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

    #[arg(long, env = "METRICS_AGENT_UI_ENABLED", default_value_t = true)]
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

    #[arg(long, env = "RUSH_REMOTE_WRITE_TOKEN")]
    pub rush_remote_write_token: Option<String>,

    #[arg(long, env = "RUSH_REMOTE_WRITE_TENANT")]
    pub rush_remote_write_tenant: Option<String>,

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

    use super::{Config, parse_duration, parse_listen_address};

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
            "--rush-remote-write-token",
            "secret-token",
            "--rush-remote-write-tenant",
            "tenant-a",
            "--scrape-enabled",
            "--scrape-interval",
            "250ms",
            "--scrape-timeout",
            "1s",
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
        assert_eq!(
            config.rush_remote_write_token.as_deref(),
            Some("secret-token")
        );
        assert_eq!(config.rush_remote_write_tenant.as_deref(), Some("tenant-a"));
        assert!(config.scrape_enabled);
        assert_eq!(config.scrape_interval, Duration::from_millis(250));
        assert_eq!(config.scrape_timeout, Duration::from_secs(1));
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
        assert_eq!(config.scrape_interval, Duration::from_secs(15));
        assert_eq!(config.scrape_timeout, Duration::from_secs(10));
        assert_eq!(config.workers, 2);
        assert!(config.ui_enabled);
        assert!(config.scrape_enabled);
        assert!(Config::try_parse_from(["metrics-agent", "--workers", "not-a-number"]).is_err());
        assert!(
            Config::try_parse_from(["metrics-agent", "--scrape-timeout", "not-a-duration"])
                .is_err()
        );
        assert!(Config::try_parse_from(["metrics-agent", "--ui-enabled=maybe"]).is_err());
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

        let config = Config::try_parse_from(["metrics-agent"]).unwrap();
        assert!(!config.ui_enabled);
        assert_eq!(config.scrape_interval, Duration::from_millis(275));
        assert_eq!(config.rush_remote_write_interval, Duration::from_secs(120));
        assert_eq!(
            config.rush_remote_write_url.as_deref(),
            Some("http://rush.test/write")
        );
    }
}
