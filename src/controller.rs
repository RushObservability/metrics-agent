use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use kube::runtime::watcher::{self, Event};
use kube::{
    api::{Api, ApiResource, DynamicObject, ListParams, Patch, PatchParams, ResourceExt},
    core::GroupVersionKind,
};
use tokio::{sync::mpsc, task::JoinHandle, time};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    config::Config,
    precedence::{
        self, PREFER_PROMETHEUS, PREFER_SOURCE_ANNOTATION, PREFER_VICTORIA_METRICS, PrecedenceInput,
    },
    remote_write::ScrapeMessage,
    status::StatusStore,
};

#[derive(Clone, Debug)]
pub struct ResourcePair {
    pub name: &'static str,
    pub prometheus: ApiResource,
    pub prometheus_kind: &'static str,
    pub victoria: ApiResource,
}

pub fn resource_pairs() -> Vec<ResourcePair> {
    vec![
        pair(
            "service-monitor",
            "v1",
            "servicemonitors",
            "ServiceMonitor",
            "v1beta1",
            "vmservicescrapes",
            "VMServiceScrape",
        ),
        pair(
            "pod-monitor",
            "v1",
            "podmonitors",
            "PodMonitor",
            "v1beta1",
            "vmpodscrapes",
            "VMPodScrape",
        ),
        pair(
            "probe", "v1", "probes", "Probe", "v1beta1", "vmprobes", "VMProbe",
        ),
        pair(
            "scrape-config",
            "v1alpha1",
            "scrapeconfigs",
            "ScrapeConfig",
            "v1beta1",
            "vmscrapeconfigs",
            "VMScrapeConfig",
        ),
    ]
}

fn pair(
    name: &'static str,
    prometheus_version: &'static str,
    prometheus_plural: &'static str,
    prometheus_kind: &'static str,
    victoria_version: &'static str,
    victoria_plural: &'static str,
    victoria_kind: &'static str,
) -> ResourcePair {
    ResourcePair {
        name,
        prometheus: api_resource(
            "monitoring.coreos.com",
            prometheus_version,
            prometheus_plural,
            prometheus_kind,
        ),
        prometheus_kind,
        victoria: api_resource(
            "operator.victoriametrics.com",
            victoria_version,
            victoria_plural,
            victoria_kind,
        ),
    }
}

fn api_resource(group: &str, version: &str, plural: &str, kind: &str) -> ApiResource {
    let mut resource = ApiResource::from_gvk(&GroupVersionKind::gvk(group, version, kind));
    resource.plural = plural.to_string();
    resource
}

#[derive(Clone, Debug)]
struct ReconcileKey {
    pair: usize,
    namespace: String,
    name: String,
}

struct ReconciliationPlan {
    decision: precedence::PrecedenceDecision,
    patch: serde_json::Value,
}

fn reconciliation_plan(
    annotations: Option<&BTreeMap<String, String>>,
    owners: &[k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference],
    prometheus_kind: &str,
    object_name: &str,
    prometheus_exists: bool,
) -> Option<ReconciliationPlan> {
    let explicit_preference = annotations.and_then(|annotations| {
        annotations
            .get(PREFER_SOURCE_ANNOTATION)
            .map(String::as_str)
    });
    let has_owner = precedence::has_prometheus_owner(owners, prometheus_kind, object_name);
    let decision = precedence::decide(PrecedenceInput {
        explicit_preference,
        has_prometheus_owner: has_owner,
        prometheus_exists,
    });
    let currently_ignored = annotations
        .and_then(|annotations| annotations.get(precedence::IGNORE_PROMETHEUS_UPDATES_ANNOTATION))
        .map(String::as_str)
        == Some(precedence::IGNORE_PROMETHEUS_UPDATES_ENABLED);
    let remove_owner = explicit_preference == Some(PREFER_VICTORIA_METRICS) && has_owner;
    if currently_ignored == decision.ignore_prometheus_updates && !remove_owner {
        return None;
    }

    let owner_references = if remove_owner {
        precedence::filter_prometheus_owner(owners, prometheus_kind, object_name)
    } else {
        owners.to_vec()
    };
    Some(ReconciliationPlan {
        patch: precedence::patch(&decision, remove_owner, &owner_references),
        decision,
    })
}

pub struct Controller {
    client: kube::Client,
    config: Config,
    pairs: Vec<ResourcePair>,
    pub status: Arc<StatusStore>,
    pub scrape_sender: tokio::sync::mpsc::Sender<ScrapeMessage>,
}

impl Controller {
    pub fn new(
        client: kube::Client,
        config: Config,
        scrape_sender: tokio::sync::mpsc::Sender<ScrapeMessage>,
    ) -> Arc<Self> {
        let pairs = resource_pairs();
        let status = StatusStore::new(
            config.agent_version.clone(),
            config.ui_enabled,
            config.ui_address.clone(),
            config.ui_path.clone(),
            config
                .rush_remote_write_url
                .clone()
                .filter(|url| !url.trim().is_empty()),
        );
        Arc::new(Self {
            client,
            config,
            pairs,
            status,
            scrape_sender,
        })
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn kube_client(&self) -> kube::Client {
        self.client.clone()
    }

    pub fn resource_pairs(&self) -> &[ResourcePair] {
        &self.pairs
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> Result<()> {
        if self.config.workers == 0 {
            return Err(anyhow!("workers must be at least 1"));
        }
        let names = self.pairs.iter().map(|pair| pair.name).collect::<Vec<_>>();
        self.status.initialize_pairs(&names).await;
        let (queue_tx, queue_rx) = mpsc::channel::<ReconcileKey>(4096);
        let mut tasks = Vec::new();

        for (pair_index, pair) in self.pairs.iter().enumerate() {
            let vm_api = Api::<DynamicObject>::all_with(self.client.clone(), &pair.victoria);
            let vm_objects = list_objects(&vm_api)
                .await
                .with_context(|| format!("list required {} CRD", pair.name))?;
            self.status
                .set_source_available(pair_index, false, true)
                .await;
            self.status
                .replace_objects(
                    pair_index,
                    false,
                    object_entries(&vm_objects, pair.prometheus_kind),
                )
                .await;
            self.spawn_watcher(
                pair_index,
                false,
                vm_api,
                queue_tx.clone(),
                shutdown.clone(),
                &mut tasks,
            );

            let prom_api = Api::<DynamicObject>::all_with(self.client.clone(), &pair.prometheus);
            match list_objects(&prom_api).await {
                Ok(prom_objects) => {
                    self.status
                        .set_source_available(pair_index, true, true)
                        .await;
                    self.status
                        .replace_objects(
                            pair_index,
                            true,
                            object_entries(&prom_objects, pair.prometheus_kind),
                        )
                        .await;
                    self.spawn_watcher(
                        pair_index,
                        true,
                        prom_api,
                        queue_tx.clone(),
                        shutdown.clone(),
                        &mut tasks,
                    );
                    info!(
                        resource_pair = pair.name,
                        "watching Prometheus and VictoriaMetrics scrape CRDs"
                    );
                }
                Err(error) if is_not_found(&error) => {
                    self.status
                        .set_source_available(pair_index, true, false)
                        .await;
                    info!(
                        resource_pair = pair.name,
                        "Prometheus scrape CRD is not installed; watching native VictoriaMetrics objects only"
                    );
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("list optional {} Prometheus CRD", pair.name));
                }
            }
        }

        self.status.set_ready(true).await;
        info!(
            resource_pairs = self.pairs.len(),
            "metrics-agent informer caches synchronized"
        );

        if self.config.scrape_enabled {
            tasks.push(tokio::spawn(crate::scraper::run(
                Arc::clone(&self),
                self.scrape_sender.clone(),
                shutdown.clone(),
            )));
        }

        let queue_rx = Arc::new(tokio::sync::Mutex::new(queue_rx));
        for _ in 0..self.config.workers {
            tasks.push(self.spawn_worker(queue_rx.clone(), shutdown.clone()));
        }
        tasks.push(self.spawn_resync(queue_tx.clone(), shutdown.clone()));
        shutdown.cancelled().await;
        self.status.set_ready(false).await;
        for task in tasks {
            task.abort();
        }
        Ok(())
    }

    fn spawn_watcher(
        &self,
        pair_index: usize,
        prometheus: bool,
        api: Api<DynamicObject>,
        queue: mpsc::Sender<ReconcileKey>,
        shutdown: CancellationToken,
        tasks: &mut Vec<JoinHandle<()>>,
    ) {
        let status = self.status.clone();
        let pair_name = self.pairs[pair_index].name;
        let pair_kind = self.pairs[pair_index].prometheus_kind;
        tasks.push(tokio::spawn(async move {
            loop {
                let stream = watcher::watcher(api.clone(), watcher::Config::default());
                tokio::pin!(stream);
                while let Some(event) = tokio::select! {
                    _ = shutdown.cancelled() => return,
                    event = stream.next() => event,
                } {
                    match event {
                        Ok(Event::Apply(object) | Event::InitApply(object)) => {
                            let key = object_key(&object);
                            status
                                .upsert_object(
                                    pair_index,
                                    prometheus,
                                    key,
                                    !prometheus
                                        && has_prometheus_owner_from_meta(
                                            &object,
                                            pair_kind,
                                            &object.name_any(),
                                        ),
                                    serialized_size(&object),
                                )
                                .await;
                            send_key(&queue, pair_index, &object).await;
                        }
                        Ok(Event::Delete(object)) => {
                            let key = object_key(&object);
                            status.remove_object(pair_index, prometheus, &key).await;
                            send_key(&queue, pair_index, &object).await;
                        }
                        Ok(Event::Init) | Ok(Event::InitDone) => {}
                        Err(error) => {
                            error!(resource_pair = pair_name, prometheus, error = %error, "scrape CRD watcher failed; retrying");
                            status.counters.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            break;
                        }
                    }
                }
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
            }
        }));
    }

    fn spawn_worker(
        self: &Arc<Self>,
        queue: Arc<tokio::sync::Mutex<mpsc::Receiver<ReconcileKey>>>,
        shutdown: CancellationToken,
    ) -> JoinHandle<()> {
        let controller = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let key = {
                    let mut receiver = queue.lock().await;
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        key = receiver.recv() => key,
                    }
                };
                let Some(key) = key else { return };
                if let Err(error) = controller.reconcile(key.clone()).await {
                    controller
                        .status
                        .counters
                        .errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    warn!(pair = controller.pairs[key.pair].name, namespace = key.namespace, name = key.name, error = %error, "precedence reconciliation failed");
                }
            }
        })
    }

    fn spawn_resync(
        self: &Arc<Self>,
        queue: mpsc::Sender<ReconcileKey>,
        shutdown: CancellationToken,
    ) -> JoinHandle<()> {
        let controller = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = time::interval(controller.config.resync_period);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = interval.tick() => {
                        for (pair_index, pair) in controller.pairs.iter().enumerate() {
                            let vm_api = Api::<DynamicObject>::all_with(controller.client.clone(), &pair.victoria);
                            match list_objects(&vm_api).await {
                                Ok(objects) => {
                                    controller.status.replace_objects(pair_index, false, object_entries(&objects, pair.prometheus_kind)).await;
                                    for object in objects { send_key(&queue, pair_index, &object).await; }
                                }
                                Err(error) => {
                                    controller.status.counters.errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                    debug!(resource_pair = pair.name, error = %error, "resync list failed");
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    async fn reconcile(&self, key: ReconcileKey) -> Result<()> {
        use std::sync::atomic::Ordering;
        self.status
            .counters
            .reconciliations
            .fetch_add(1, Ordering::Relaxed);
        let pair = &self.pairs[key.pair];
        let vm_api = Api::<DynamicObject>::namespaced_with(
            self.client.clone(),
            &key.namespace,
            &pair.victoria,
        );
        let Some(vm_object) = vm_api.get_opt(&key.name).await? else {
            return Ok(());
        };
        let prom_exists =
            if self.status.snapshot().await.resource_pairs[key.pair].prometheus_available {
                let prom_api = Api::<DynamicObject>::namespaced_with(
                    self.client.clone(),
                    &key.namespace,
                    &pair.prometheus,
                );
                prom_api.get_opt(&key.name).await?.is_some()
            } else {
                false
            };
        let annotations = vm_object.metadata.annotations.as_ref();
        let owners = vm_object
            .metadata
            .owner_references
            .as_deref()
            .unwrap_or_default();
        let Some(plan) = reconciliation_plan(
            annotations,
            owners,
            pair.prometheus_kind,
            &key.name,
            prom_exists,
        ) else {
            return Ok(());
        };
        vm_api
            .patch(
                &key.name,
                &PatchParams::default(),
                &Patch::Merge(plan.patch),
            )
            .await?;
        if plan.decision.ignore_prometheus_updates {
            self.status
                .counters
                .patches_victoria
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.status
                .counters
                .patches_prometheus
                .fetch_add(1, Ordering::Relaxed);
        }
        info!(
            resource_pair = pair.name,
            namespace = key.namespace,
            name = key.name,
            source = if plan.decision.ignore_prometheus_updates {
                PREFER_VICTORIA_METRICS
            } else {
                PREFER_PROMETHEUS
            },
            reason = plan.decision.reason,
            "updated scrape configuration precedence"
        );
        Ok(())
    }
}

async fn list_objects(api: &Api<DynamicObject>) -> Result<Vec<DynamicObject>, kube::Error> {
    Ok(api.list(&ListParams::default()).await?.items)
}

fn is_not_found(error: &kube::Error) -> bool {
    matches!(error, kube::Error::Api(response) if response.code == 404)
}

fn object_key(object: &DynamicObject) -> String {
    format!(
        "{}/{}",
        object.namespace().unwrap_or_default(),
        object.name_any()
    )
}

fn object_entries(objects: &[DynamicObject], kind: &str) -> Vec<(String, bool, u64)> {
    objects
        .iter()
        .map(|object| {
            (
                object_key(object),
                has_prometheus_owner_from_meta(object, kind, &object.name_any()),
                serialized_size(object),
            )
        })
        .collect()
}

fn serialized_size(object: &DynamicObject) -> u64 {
    serde_json::to_vec(object)
        .map(|bytes| bytes.len() as u64)
        .unwrap_or_default()
}

fn has_prometheus_owner_from_meta(object: &DynamicObject, kind: &str, name: &str) -> bool {
    precedence::has_prometheus_owner(
        object
            .metadata
            .owner_references
            .as_deref()
            .unwrap_or_default(),
        kind,
        name,
    )
}

async fn send_key(queue: &mpsc::Sender<ReconcileKey>, pair: usize, object: &DynamicObject) {
    let Some(namespace) = object.namespace() else {
        return;
    };
    let key = ReconcileKey {
        pair,
        namespace,
        name: object.name_any(),
    };
    if queue.send(key).await.is_err() {
        debug!("reconciliation queue closed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::precedence::{
        IGNORE_PROMETHEUS_UPDATES_ANNOTATION, IGNORE_PROMETHEUS_UPDATES_ENABLED,
    };

    const VM_SERVICE_SCRAPE_FIXTURE: &str = r#"
    {
      "apiVersion": "operator.victoriametrics.com/v1beta1",
      "kind": "VMServiceScrape",
      "metadata": {
        "name": "payments",
        "namespace": "observability",
        "annotations": {
          "team": "platform",
          "metrics-agent.rushobservability.com/prefer-source": "prometheus"
        },
        "ownerReferences": [{
          "apiVersion": "monitoring.coreos.com/v1",
          "kind": "ServiceMonitor",
          "name": "payments",
          "uid": "sm-uid",
          "controller": true
        }]
      },
      "spec": {
        "endpoints": [{"port": "http", "interval": "30s"}],
        "selector": {"matchLabels": {"app": "payments"}}
      }
    }
    "#;

    fn fixture() -> DynamicObject {
        serde_json::from_str(VM_SERVICE_SCRAPE_FIXTURE).expect("valid DynamicObject fixture")
    }

    fn owner(
        api_version: &str,
        kind: &str,
        name: &str,
    ) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
        k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            name: name.to_string(),
            uid: format!("uid-{name}"),
            block_owner_deletion: None,
            controller: None,
        }
    }

    #[test]
    fn resource_pairs_expose_stable_discovery_metadata() {
        let pairs = resource_pairs();
        assert_eq!(
            pairs.iter().map(|pair| pair.name).collect::<Vec<_>>(),
            vec!["service-monitor", "pod-monitor", "probe", "scrape-config"]
        );
        let expected = [
            (
                "v1",
                "servicemonitors",
                "ServiceMonitor",
                "vmservicescrapes",
                "VMServiceScrape",
            ),
            (
                "v1",
                "podmonitors",
                "PodMonitor",
                "vmpodscrapes",
                "VMPodScrape",
            ),
            ("v1", "probes", "Probe", "vmprobes", "VMProbe"),
            (
                "v1alpha1",
                "scrapeconfigs",
                "ScrapeConfig",
                "vmscrapeconfigs",
                "VMScrapeConfig",
            ),
        ];
        for (
            pair,
            (
                prometheus_version,
                prometheus_plural,
                prometheus_kind,
                victoria_plural,
                victoria_kind,
            ),
        ) in pairs.iter().zip(expected)
        {
            assert_eq!(pair.prometheus.group, "monitoring.coreos.com");
            assert_eq!(pair.prometheus.version, prometheus_version);
            assert_eq!(pair.prometheus.plural, prometheus_plural);
            assert_eq!(pair.prometheus.kind, prometheus_kind);
            assert_eq!(pair.prometheus_kind, prometheus_kind);
            assert_eq!(pair.victoria.group, "operator.victoriametrics.com");
            assert_eq!(pair.victoria.version, "v1beta1");
            assert_eq!(pair.victoria.plural, victoria_plural);
            assert_eq!(pair.victoria.kind, victoria_kind);
        }
    }

    #[test]
    fn object_key_and_memory_estimate_use_the_serialized_object() {
        let object = fixture();
        assert_eq!(object_key(&object), "observability/payments");
        assert_eq!(
            serialized_size(&object),
            serde_json::to_vec(&object).unwrap().len() as u64
        );
        assert!(serialized_size(&object) > 0);

        let entries = object_entries(&[object], "ServiceMonitor");
        assert_eq!(entries.len(), 1);
        assert!(entries[0].1);
        assert!(entries[0].2 > 0);
    }

    #[test]
    fn object_entries_do_not_treat_unrelated_owner_as_prometheus_conversion() {
        let mut object = fixture();
        object.metadata.owner_references = Some(vec![owner("apps/v1", "Deployment", "payments")]);
        let entries = object_entries(&[object], "ServiceMonitor");
        assert!(!entries[0].1);
    }

    #[test]
    fn reconciliation_is_a_noop_when_annotation_already_matches() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            IGNORE_PROMETHEUS_UPDATES_ANNOTATION.to_string(),
            IGNORE_PROMETHEUS_UPDATES_ENABLED.to_string(),
        );
        let owners = vec![owner("apps/v1", "Deployment", "payments")];

        assert!(
            reconciliation_plan(
                Some(&annotations),
                &owners,
                "ServiceMonitor",
                "payments",
                false,
            )
            .is_none()
        );
    }

    #[test]
    fn reconciliation_patches_converted_objects_to_prometheus_precedence() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            IGNORE_PROMETHEUS_UPDATES_ANNOTATION.to_string(),
            IGNORE_PROMETHEUS_UPDATES_ENABLED.to_string(),
        );
        let owners = vec![owner(
            "monitoring.coreos.com/v1",
            "ServiceMonitor",
            "payments",
        )];
        let plan = reconciliation_plan(
            Some(&annotations),
            &owners,
            "ServiceMonitor",
            "payments",
            false,
        )
        .expect("converted object should be patched");

        assert!(!plan.decision.ignore_prometheus_updates);
        assert_eq!(plan.decision.reason, "prometheus-converted-object");
        assert!(
            plan.patch["metadata"]["annotations"][IGNORE_PROMETHEUS_UPDATES_ANNOTATION].is_null()
        );
        assert!(plan.patch["metadata"].get("ownerReferences").is_none());
    }

    #[test]
    fn force_victoria_removes_only_matching_prometheus_owner_and_preserves_annotations() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            PREFER_SOURCE_ANNOTATION.to_string(),
            PREFER_VICTORIA_METRICS.to_string(),
        );
        annotations.insert("team".to_string(), "platform".to_string());
        let owners = vec![
            owner("monitoring.coreos.com/v1", "ServiceMonitor", "payments"),
            owner("apps/v1", "Deployment", "payments"),
        ];
        let plan = reconciliation_plan(
            Some(&annotations),
            &owners,
            "ServiceMonitor",
            "payments",
            true,
        )
        .expect("force-victoria must remove converted owner");

        assert!(plan.decision.ignore_prometheus_updates);
        assert_eq!(plan.decision.reason, "explicit-victoriametrics-preference");
        assert_eq!(
            plan.patch["metadata"]["ownerReferences"],
            serde_json::json!([owner("apps/v1", "Deployment", "payments")])
        );
        assert_eq!(
            plan.patch["metadata"]["annotations"][IGNORE_PROMETHEUS_UPDATES_ANNOTATION],
            IGNORE_PROMETHEUS_UPDATES_ENABLED
        );
        assert!(plan.patch["metadata"]["annotations"].get("team").is_none());
    }

    #[test]
    fn send_key_skips_cluster_scoped_objects_and_enqueues_namespaced_objects() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let (tx, mut rx) = mpsc::channel(1);
            let mut cluster_scoped = fixture();
            cluster_scoped.metadata.namespace = None;
            send_key(&tx, 2, &cluster_scoped).await;
            assert!(rx.try_recv().is_err());

            send_key(&tx, 2, &fixture()).await;
            let key = rx.recv().await.expect("namespaced object should enqueue");
            assert_eq!(key.pair, 2);
            assert_eq!(key.namespace, "observability");
            assert_eq!(key.name, "payments");
        });
    }

    #[test]
    fn not_found_detection_only_matches_api_404() {
        let not_found = kube::Error::Api(kube::error::ErrorResponse {
            status: "Failure".to_string(),
            message: "missing".to_string(),
            reason: "NotFound".to_string(),
            code: 404,
        });
        let forbidden = kube::Error::Api(kube::error::ErrorResponse {
            status: "Failure".to_string(),
            message: "denied".to_string(),
            reason: "Forbidden".to_string(),
            code: 403,
        });
        assert!(is_not_found(&not_found));
        assert!(!is_not_found(&forbidden));
    }
}
