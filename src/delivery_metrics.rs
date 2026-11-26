use crate::{metrics::instruments::Registry, remote_write::ScrapeMessage};
use std::{sync::OnceLock, time::Instant};
#[derive(Default)]
pub(crate) struct DeliveryMetrics(pub Registry);
pub(crate) fn global() -> &'static DeliveryMetrics {
    static METRICS: OnceLock<DeliveryMetrics> = OnceLock::new();
    METRICS.get_or_init(DeliveryMetrics::default)
}
impl DeliveryMetrics {
    pub fn retry(&self, outcome: &'static str) {
        self.0.counter(
            "metrics_agent_remote_write_retries_total",
            &[("outcome", outcome)],
            1,
        );
    }

    pub fn discarded(&self, samples: usize, reason: &'static str) {
        self.0.counter(
            "metrics_agent_remote_write_samples_total",
            &[("outcome", "discarded")],
            samples as u64,
        );
        self.0.counter(
            "metrics_agent_remote_write_dropped_batches_total",
            &[("reason", reason)],
            1,
        );
    }
    pub fn attempt(&self, samples: usize) -> Attempt<'_> {
        Attempt {
            metrics: self,
            samples,
            started: Instant::now(),
            outcome: "cancelled",
        }
    }
}
pub(crate) struct Attempt<'a> {
    metrics: &'a DeliveryMetrics,
    samples: usize,
    started: Instant,
    outcome: &'static str,
}
impl Attempt<'_> {
    pub fn finish(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}
impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        self.metrics.0.counter(
            "metrics_agent_remote_write_requests_total",
            &[("outcome", self.outcome)],
            1,
        );
        self.metrics.0.observe(
            "metrics_agent_remote_write_request_duration_seconds",
            &[],
            self.started.elapsed().as_secs_f64(),
        );
        if self.samples > 0 {
            match self.outcome {
                "success" => self.metrics.0.counter(
                    "metrics_agent_remote_write_samples_total",
                    &[("outcome", "sent")],
                    self.samples as u64,
                ),
                "timeout" | "transport_error" | "cancelled" => {
                    self.metrics.0.counter(
                        "metrics_agent_remote_write_samples_total",
                        &[("outcome", "unconfirmed")],
                        self.samples as u64,
                    );
                    self.metrics.0.counter(
                        "metrics_agent_remote_write_dropped_batches_total",
                        &[("reason", self.outcome)],
                        1,
                    );
                }
                _ => self.metrics.discarded(self.samples, "send_failed"),
            }
        }
    }
}
pub(crate) struct Receiver<'a> {
    pub inner: tokio::sync::mpsc::Receiver<ScrapeMessage>,
    pub metrics: &'a DeliveryMetrics,
}
impl Drop for Receiver<'_> {
    fn drop(&mut self) {
        self.inner.close();
        while let Ok(message) = self.inner.try_recv() {
            if let ScrapeMessage::Batch(samples) = message {
                self.metrics.discarded(samples.len(), "receiver_closed");
            }
        }
        crate::queue_metrics::closed(crate::queue_metrics::Queue::Publish);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_write::{CollectedMetric, MetricType};
    fn sample() -> CollectedMetric {
        CollectedMetric {
            name: "test_sample".into(),
            labels: vec![],
            value: 1.0,
            timestamp: 1,
            help: String::new(),
            metric_type: MetricType::Gauge,
        }
    }
    #[test]
    fn cancellation_and_closed_channel_preserve_sample_accounting() {
        let metrics = DeliveryMetrics::default();
        drop(metrics.attempt(3));
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.try_send(ScrapeMessage::Batch(vec![sample(), sample()]))
            .unwrap();
        drop(Receiver {
            inner: rx,
            metrics: &metrics,
        });
        let rendered = metrics.0.render();
        assert!(rendered.contains("samples_total{outcome=\"unconfirmed\"} 3"));
        assert!(rendered.contains("samples_total{outcome=\"discarded\"} 2"));
        assert!(rendered.contains("requests_total{outcome=\"cancelled\"} 1"));
        assert!(rendered.contains("reason=\"receiver_closed\""));
    }
}
