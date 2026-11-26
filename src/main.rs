use std::{sync::Arc, time::Duration};

use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use metrics_agent::{
    config::{Config, configure_logging},
    controller::Controller,
    http, remote_write, scraper,
};

// Leave time for process exit within the chart's default 30-second grace period.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(25);

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    configure_logging(&config.log_level);
    scraper::validate_internal_metrics(&config)?;
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
    let mut remote_write_task = {
        let status = Arc::clone(&controller.status);
        let token = config.rush_remote_write_token.clone();
        let tenant = config.rush_remote_write_tenant.clone();
        let extra_labels = config.extra_labels.clone();
        let interval = config.rush_remote_write_interval;
        let client = remote_write::http_client(
            config.rush_remote_write_connect_timeout,
            config.rush_remote_write_timeout,
        )?;
        let shutdown = shutdown.clone();
        metrics_agent::worker_metrics::spawn(
            metrics_agent::worker_metrics::Worker::RemoteWriter,
            shutdown.clone(),
            remote_write::run(
                client,
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
            ),
        )
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
    let primary_router = http::primary_router(Arc::clone(&controller))?;
    let server = axum::serve(listener, primary_router)
        .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
        .into_future();
    tokio::pin!(server);

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let terminate = async {
        #[cfg(unix)]
        {
            sigterm.recv().await;
        }
        #[cfg(not(unix))]
        std::future::pending::<()>().await;
    };
    let mut server_finished = false;
    let mut remote_writer_finished = false;
    tokio::select! {
        result = &mut server => {
            server_finished = true;
            if let Err(error) = result { error!(%error, "HTTP server stopped unexpectedly"); }
        }
        result = controller_task => {
            match result {
                Ok(Ok(())) => info!("controller stopped"),
                Ok(Err(error)) => error!(%error, "controller stopped with an error"),
                Err(error) => error!(%error, "controller task panicked"),
            }
        }
        result = &mut remote_write_task => {
            remote_writer_finished = true;
            match result { Ok(()) => error!("remote writer stopped unexpectedly"), Err(error) => error!(%error, "remote writer task failed") }
        }
        _ = tokio::signal::ctrl_c() => info!("interrupt received"),
        _ = terminate => info!("termination signal received"),
    }

    shutdown.cancel();
    let graceful_shutdown = async {
        tokio::join!(
            async {
                if !server_finished {
                    let _ = server.await;
                }
            },
            async {
                if let Some(server) = ui_server.as_mut() {
                    let _ = server.await;
                }
            },
            async {
                if !remote_writer_finished {
                    if let Err(error) = (&mut remote_write_task).await {
                        error!(%error, "remote writer task failed during shutdown");
                    }
                }
            },
        );
    };
    if tokio::time::timeout(SHUTDOWN_GRACE, graceful_shutdown)
        .await
        .is_err()
    {
        warn!("shutdown grace period expired");
        remote_write_task.abort();
        if let Some(server) = ui_server {
            server.abort();
        }
    }
    Ok(())
}
