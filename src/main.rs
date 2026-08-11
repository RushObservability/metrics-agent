use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use metrics_agent::{
    config::{Config, configure_logging},
    controller::Controller,
    http, remote_write,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    configure_logging(&config.log_level);
    info!(version = %config.agent_version, http_address = %config.http_address, ui_enabled = config.ui_enabled, ui_address = %config.ui_address, "starting metrics-agent");
    if config.ui_enabled {
        info!(path = %config.ui_path, "embedded metrics-agent UI enabled");
    }

    let kube_config = config.kube_config().await?;
    let client = kube::Client::try_from(kube_config)?;
    let (scrape_sender, scrape_receiver) = tokio::sync::mpsc::channel(2);
    let controller = Controller::new(client, config.clone(), scrape_sender);
    let shutdown = CancellationToken::new();
    let controller_shutdown = shutdown.clone();
    let controller_task = {
        let controller = Arc::clone(&controller);
        tokio::spawn(async move { controller.run(controller_shutdown).await })
    };
    let remote_write_task = {
        let status = Arc::clone(&controller.status);
        let token = config.rush_remote_write_token.clone();
        let tenant = config.rush_remote_write_tenant.clone();
        let extra_labels = config.extra_labels.clone();
        let interval = config.rush_remote_write_interval;
        let shutdown = shutdown.clone();
        tokio::spawn(remote_write::run(
            reqwest::Client::new(),
            status,
            scrape_receiver,
            config
                .rush_remote_write_url
                .clone()
                .filter(|url| !url.trim().is_empty()),
            token,
            tenant,
            extra_labels,
            interval,
            shutdown,
        ))
    };

    let address = config.http_socket_addr()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    let mut ui_server = None;
    if config.ui_enabled {
        let ui_address = config.ui_socket_addr()?;
        if ui_address != address {
            let ui_listener = tokio::net::TcpListener::bind(ui_address).await?;
            let ui_controller = Arc::clone(&controller);
            let ui_shutdown = shutdown.clone();
            ui_server = Some(tokio::spawn(async move {
                axum::serve(ui_listener, http::router(ui_controller))
                    .with_graceful_shutdown(async move { ui_shutdown.cancelled().await })
                    .await
            }));
            info!(address = %ui_address, "embedded metrics-agent UI listener started");
        }
    }
    let server_shutdown = shutdown.clone();
    let server = axum::serve(listener, http::router(Arc::clone(&controller)))
        .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => {
            if let Err(error) = result { error!(%error, "HTTP server stopped unexpectedly"); }
            shutdown.cancel();
        }
        result = controller_task => {
            match result {
                Ok(Ok(())) => info!("controller stopped"),
                Ok(Err(error)) => error!(%error, "controller stopped with an error"),
                Err(error) => error!(%error, "controller task panicked"),
            }
            shutdown.cancel();
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
            shutdown.cancel();
        }
    }

    let _ = server.await;
    if let Some(ui_server) = ui_server {
        ui_server.abort();
        let _ = ui_server.await;
    }
    remote_write_task.abort();
    let _ = remote_write_task.await;
    Ok(())
}
