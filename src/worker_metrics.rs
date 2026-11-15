//! Worker supervision and progress, with fixed process-level dimensions.
use crate::metrics::instruments::Registry;
use std::{
    future::Future,
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
#[derive(Clone, Copy)]
pub enum Worker {
    Watcher,
    Reconciler,
    Resync,
    Scraper,
    RemoteWriter,
}
impl Worker {
    fn label(self) -> &'static str {
        match self {
            Self::Watcher => "watcher",
            Self::Reconciler => "reconciler",
            Self::Resync => "resync",
            Self::Scraper => "scraper",
            Self::RemoteWriter => "remote_writer",
        }
    }
}
pub(crate) fn registry() -> &'static Registry {
    static METRICS: OnceLock<Registry> = OnceLock::new();
    METRICS.get_or_init(|| {
        let registry = Registry::default();
        for worker in [
            Worker::Watcher,
            Worker::Reconciler,
            Worker::Resync,
            Worker::Scraper,
            Worker::RemoteWriter,
        ] {
            registry.gauge(
                "metrics_agent_workers_running",
                &[("worker", worker.label())],
                0.0,
            );
        }
        for stage in ["discovery", "scrape", "workload_publish", "self_publish"] {
            registry.gauge(
                "metrics_agent_last_success_timestamp_seconds",
                &[("stage", stage)],
                0.0,
            );
        }
        registry
    })
}
pub(crate) fn success(stage: &'static str) {
    registry().gauge(
        "metrics_agent_last_success_timestamp_seconds",
        &[("stage", stage)],
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
    );
}
struct Guard {
    worker: Worker,
    shutdown: CancellationToken,
}
impl Drop for Guard {
    fn drop(&mut self) {
        registry().add_gauge(
            "metrics_agent_workers_running",
            &[("worker", self.worker.label())],
            -1.0,
        );
        let reason = if std::thread::panicking() {
            "panic"
        } else if self.shutdown.is_cancelled() {
            "shutdown"
        } else {
            "unexpected_exit"
        };
        registry().counter(
            "metrics_agent_worker_exits_total",
            &[("worker", self.worker.label()), ("reason", reason)],
            1,
        );
    }
}
pub fn spawn<F>(worker: Worker, shutdown: CancellationToken, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(async move {
        registry().add_gauge(
            "metrics_agent_workers_running",
            &[("worker", worker.label())],
            1.0,
        );
        let _guard = Guard { worker, shutdown };
        future.await
    })
}
pub(crate) async fn supervise(
    tasks: &mut [JoinHandle<()>],
    shutdown: &CancellationToken,
) -> anyhow::Result<()> {
    let exited = tokio::select! {
        _ = shutdown.cancelled() => None,
        (result, _, _) = futures_util::future::select_all(tasks.iter_mut()) => Some(result),
    };
    shutdown.cancel();
    for task in tasks {
        task.abort();
    }
    if let Some(result) = exited {
        match result {
            Ok(()) => anyhow::bail!("controller worker exited unexpectedly"),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn unexpected_exit_cancels_siblings_and_panics_are_reported() {
        for panic in [false, true] {
            let shutdown = CancellationToken::new();
            let mut tasks = vec![
                spawn(Worker::Scraper, shutdown.clone(), async move {
                    assert!(!panic, "test child failure");
                }),
                spawn(Worker::Resync, shutdown.clone(), std::future::pending()),
            ];
            assert!(supervise(&mut tasks, &shutdown).await.is_err());
            assert!(shutdown.is_cancelled());
        }
        let text = registry().render();
        assert!(text.contains("reason=\"unexpected_exit\""));
        assert!(text.contains("reason=\"panic\""));
    }
    #[tokio::test]
    async fn requested_shutdown_is_not_an_unexpected_exit() {
        let shutdown = CancellationToken::new();
        let mut tasks = vec![spawn(
            Worker::Scraper,
            shutdown.clone(),
            std::future::pending(),
        )];
        shutdown.cancel();
        supervise(&mut tasks, &shutdown).await.unwrap();
    }
}
