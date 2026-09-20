#[test]
fn cli_help_does_not_print_credentials_from_environment() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_metrics-agent"))
        .arg("--help")
        .env("RUSH_REMOTE_WRITE_TOKEN", "dummy-help-token")
        .env(
            "RUSH_REMOTE_WRITE_URL",
            "https://test.invalid/write?key=dummy-url-key",
        )
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("RUSH_REMOTE_WRITE_TOKEN"));
    assert!(!help.contains("dummy-help-token"));
    assert!(!help.contains("dummy-url-key"));
}

#[test]
fn debug_output_does_not_print_credentials() {
    use clap::Parser;
    let config = metrics_agent::config::Config::try_parse_from([
        "metrics-agent",
        "--rush-remote-write-token=dummy-debug-token",
        "--rush-remote-write-url=https://test.invalid/write?key=dummy-debug-url",
        "--scrape-max-retained-bytes=4096",
    ])
    .unwrap();
    assert_eq!(config.scrape_max_retained_bytes, 4096);
    let debug = format!("{config:?}");
    assert!(!debug.contains("dummy-debug"));
    assert!(
        metrics_agent::config::Config::try_parse_from([
            "metrics-agent",
            "--scrape-max-retained-bytes=0",
        ])
        .is_err()
    );
}
