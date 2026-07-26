pub mod config;
pub mod controller;
pub mod http;
pub mod metrics;
pub mod precedence;
pub mod remote_write;
pub mod scraper;
pub mod status;

pub const DEFAULT_VERSION: &str = env!("CARGO_PKG_VERSION");
