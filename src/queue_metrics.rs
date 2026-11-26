use crate::{metrics::instruments::Registry, remote_write::ScrapeMessage};
use std::{future::Future, sync::OnceLock, time::Instant};
use tokio::sync::mpsc;
pub(crate) fn registry() -> &'static Registry {
    static METRICS: OnceLock<Registry> = OnceLock::new();
    METRICS.get_or_init(Registry::default)
}
#[derive(Clone, Copy)]
pub(crate) enum Queue {
    Reconcile,
    Publish,
}
impl Queue {
    fn label(self) -> &'static str {
        match self {
            Self::Reconcile => "reconcile",
            Self::Publish => "publish",
        }
    }
}
fn sample(metrics: &Registry, queue: Queue, capacity: usize, remaining: usize) {
    metrics.gauge(
        "metrics_agent_queue_capacity",
        &[("queue", queue.label())],
        capacity as f64,
    );
    metrics.gauge(
        "metrics_agent_queue_depth",
        &[("queue", queue.label())],
        capacity.saturating_sub(remaining) as f64,
    );
}
pub(crate) fn initialize(queue: Queue, capacity: usize) {
    sample(registry(), queue, capacity, capacity);
    registry().gauge(
        "metrics_agent_queue_senders_waiting",
        &[("queue", queue.label())],
        0.0,
    );
}
pub(crate) fn closed(queue: Queue) {
    registry().gauge(
        "metrics_agent_queue_depth",
        &[("queue", queue.label())],
        0.0,
    );
}
struct Wait<'a> {
    metrics: &'a Registry,
    queue: Queue,
    started: Instant,
    outcome: &'static str,
}
impl Drop for Wait<'_> {
    fn drop(&mut self) {
        let labels = [("queue", self.queue.label())];
        self.metrics
            .add_gauge("metrics_agent_queue_senders_waiting", &labels, -1.0);
        self.metrics.observe(
            "metrics_agent_queue_send_wait_seconds",
            &labels,
            self.started.elapsed().as_secs_f64(),
        );
        self.metrics.counter(
            "metrics_agent_queue_sends_total",
            &[("queue", self.queue.label()), ("outcome", self.outcome)],
            1,
        );
    }
}
async fn send_with<T>(
    metrics: &Registry,
    sender: &mpsc::Sender<T>,
    message: T,
    queue: Queue,
) -> Result<(), mpsc::error::SendError<T>> {
    sample(
        metrics,
        queue,
        sender.max_capacity(),
        if sender.is_closed() {
            sender.max_capacity()
        } else {
            sender.capacity()
        },
    );
    metrics.add_gauge(
        "metrics_agent_queue_senders_waiting",
        &[("queue", queue.label())],
        1.0,
    );
    let mut wait = Wait {
        metrics,
        queue,
        started: Instant::now(),
        outcome: "cancelled",
    };
    let result = sender.send(message).await;
    wait.outcome = if result.is_ok() { "success" } else { "closed" };
    sample(
        metrics,
        queue,
        sender.max_capacity(),
        if sender.is_closed() {
            sender.max_capacity()
        } else {
            sender.capacity()
        },
    );
    result
}
pub(crate) async fn send<T>(
    sender: &mpsc::Sender<T>,
    message: T,
    queue: Queue,
) -> Result<(), mpsc::error::SendError<T>> {
    send_with(registry(), sender, message, queue).await
}
pub(crate) async fn recv<T>(receiver: &mut mpsc::Receiver<T>, queue: Queue) -> Option<T> {
    let message = receiver.recv().await;
    sample(
        registry(),
        queue,
        receiver.max_capacity(),
        receiver.capacity(),
    );
    message
}
struct PendingSamples {
    count: usize,
    reason: &'static str,
}
impl Drop for PendingSamples {
    fn drop(&mut self) {
        if self.count > 0 {
            crate::delivery_metrics::global().discarded(self.count, self.reason);
        }
    }
}
pub(crate) async fn publish(
    sender: &mpsc::Sender<ScrapeMessage>,
    message: ScrapeMessage,
) -> Result<(), mpsc::error::SendError<ScrapeMessage>> {
    let count = match &message {
        ScrapeMessage::Batch(samples) => samples.len(),
        _ => 0,
    };
    let mut pending = PendingSamples {
        count,
        reason: "sender_cancelled",
    };
    let result = send(sender, message, Queue::Publish).await;
    if result.is_ok() {
        pending.count = 0;
    } else {
        pending.reason = "channel_closed";
    }
    result
}
pub(crate) struct Operation {
    operation: &'static str,
    pair: &'static str,
    source: &'static str,
    outcome: &'static str,
    started: Instant,
}
impl Operation {
    pub fn new(operation: &'static str, pair: &'static str, source: &'static str) -> Self {
        Self {
            operation,
            pair,
            source,
            outcome: "cancelled",
            started: Instant::now(),
        }
    }
    pub fn finish(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        operation(self.operation, self.pair, self.source, self.outcome);
        registry().observe(
            "metrics_agent_controller_operation_duration_seconds",
            &[
                ("operation", self.operation),
                ("resource_pair", self.pair),
                ("source", self.source),
            ],
            self.started.elapsed().as_secs_f64(),
        );
    }
}
pub(crate) fn operation(
    operation: &'static str,
    pair: &'static str,
    source: &'static str,
    outcome: &'static str,
) {
    registry().counter(
        "metrics_agent_controller_operations_total",
        &[
            ("operation", operation),
            ("resource_pair", pair),
            ("source", source),
            ("outcome", outcome),
        ],
        1,
    );
}
pub(crate) async fn kube_operation<T>(
    operation: &'static str,
    pair: &'static str,
    source: &'static str,
    future: impl Future<Output = Result<T, kube::Error>>,
) -> Result<T, kube::Error> {
    let mut metric = Operation::new(operation, pair, source);
    let result = future.await;
    metric.finish(match &result {
        Ok(_) => "success",
        Err(kube::Error::Api(e)) if matches!(e.code, 401 | 403) => "forbidden",
        Err(kube::Error::Api(e)) if e.code == 404 => "not_found",
        Err(kube::Error::Api(e)) if matches!(e.code, 408 | 504) => "timeout",
        Err(_) => "error",
    });
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn blocked_send_cancellation_and_closed_receiver_release_waiters() {
        let metrics = Registry::default();
        let (sender, mut receiver) = mpsc::channel(1);
        send_with(&metrics, &sender, 1, Queue::Publish)
            .await
            .unwrap();
        {
            let blocked = send_with(&metrics, &sender, 2, Queue::Publish);
            tokio::pin!(blocked);
            assert!(futures_util::poll!(&mut blocked).is_pending());
        }
        let text = metrics.render();
        assert!(text.contains("queue_depth{queue=\"publish\"} 1"));
        assert!(text.contains("queue_senders_waiting{queue=\"publish\"} 0"));
        assert!(text.contains("outcome=\"cancelled\"} 1"));
        assert_eq!(receiver.recv().await, Some(1));
        drop(receiver);
        assert!(
            send_with(&metrics, &sender, 3, Queue::Publish)
                .await
                .is_err()
        );
        let text = metrics.render();
        assert!(text.contains("outcome=\"closed\"} 1"));
        assert!(text.contains("queue_depth{queue=\"publish\"} 0"));
    }
    #[tokio::test]
    async fn kube_outcomes_preserve_forbidden_and_cancelled_operations() {
        let error = kube::Error::Api(Box::new(
            kube::core::Status::failure("private object", "Forbidden").with_code(403),
        ));
        assert!(
            kube_operation("patch", "service-monitor", "victoria", async {
                Err::<(), _>(error)
            })
            .await
            .is_err()
        );
        {
            let pending = kube_operation(
                "list",
                "pod-monitor",
                "prometheus",
                std::future::pending::<Result<(), kube::Error>>(),
            );
            tokio::pin!(pending);
            assert!(futures_util::poll!(&mut pending).is_pending());
        }
        let text = registry().render();
        assert!(text.contains("outcome=\"forbidden\""));
        assert!(text.contains("operation=\"list\",resource_pair=\"pod-monitor\",source=\"prometheus\",outcome=\"cancelled\""));
        assert!(!text.contains("private object"));
    }
    #[tokio::test]
    async fn cancelled_publish_send_accounts_for_samples_that_never_entered_channel() {
        let (sender, receiver) = mpsc::channel(1);
        sender
            .send(ScrapeMessage::Start { targets: 1 })
            .await
            .unwrap();
        {
            let samples = crate::remote_write::parse_prometheus_text(
                "pending_workload 1\n",
                &[],
                1,
                crate::remote_write::ScrapeLimits::default(),
            )
            .unwrap();
            let blocked = publish(&sender, ScrapeMessage::Batch(samples));
            tokio::pin!(blocked);
            assert!(futures_util::poll!(&mut blocked).is_pending());
        }
        drop(receiver);
        assert!(
            crate::delivery_metrics::global()
                .0
                .render()
                .contains("reason=\"sender_cancelled\"")
        );
    }
}
