//! Watched Service, Endpoints and Pod caches for monitor discovery.
//!
//! Each kind is listed once when a monitor first needs it and then kept current
//! by a watch, so a discovery refresh reads memory instead of re-listing every
//! object in the cluster. Only the fields discovery reads are kept.

use std::time::Duration;

use anyhow::{Result, bail};
use futures_util::StreamExt;
use kube::{
    Client,
    api::{Api, ApiResource, DynamicObject, ObjectMeta},
    core::GroupVersionKind,
    runtime::{
        reflector::{Store, store::Writer},
        watcher::{self, Event},
    },
};
use serde_json::Value;
use tokio::{sync::watch, time};
use tokio_util::task::AbortOnDropHandle;
use tracing::warn;

/// Same fixed retry delay as the scrape CRD watches.
const RETRY_DELAY: Duration = Duration::from_secs(5);

pub(super) struct DiscoveryWatches {
    /// How long a refresh waits for a watch to (re)synchronize before failing.
    pub(super) sync_wait: Duration,
    pub(super) services: CoreWatch,
    pub(super) endpoints: CoreWatch,
    pub(super) pods: CoreWatch,
}

impl Default for DiscoveryWatches {
    fn default() -> Self {
        Self {
            // Matches the default scrape timeout.
            sync_wait: Duration::from_secs(10),
            services: CoreWatch::new("Service", "services"),
            endpoints: CoreWatch::new("Endpoints", "endpoints"),
            pods: CoreWatch::new("Pod", "pods"),
        }
    }
}

pub(super) struct CoreWatch {
    kind: &'static str,
    plural: &'static str,
    pub(super) running: Option<Running>,
}

pub(super) struct Running {
    store: Store<DynamicObject>,
    /// None while (re)listing, then the latest list or watch outcome.
    synced: watch::Receiver<Option<Result<(), String>>>,
    _task: AbortOnDropHandle<()>,
}

impl CoreWatch {
    fn new(kind: &'static str, plural: &'static str) -> Self {
        Self {
            kind,
            plural,
            running: None,
        }
    }

    /// Trimmed objects in list order. The watch starts on first need and is
    /// dropped, with its cache, when no monitor needs this kind.
    pub(super) async fn objects(
        &mut self,
        client: &Client,
        needed: bool,
        wait: Duration,
    ) -> Result<Vec<Value>> {
        if !needed {
            self.running = None;
            return Ok(Vec::new());
        }
        let (kind, plural) = (self.kind, self.plural);
        let running = self
            .running
            .get_or_insert_with(|| start(client, kind, plural));
        let mut synced = running.synced.clone();
        // Never report a cache that has not finished its initial list, or one
        // whose watch failed, as an empty cluster: fail so the bounded previous
        // targets are kept instead.
        let ready = synced.wait_for(|synced| matches!(synced, Some(Ok(()))));
        if !matches!(time::timeout(wait, ready).await, Ok(Ok(_))) {
            if let Some(Err(error)) = &*synced.borrow() {
                bail!("Kubernetes {kind} watch failed: {error}");
            }
            bail!("Kubernetes {kind} watch is not synchronized");
        }
        let mut objects = running.store.state();
        // Match the namespace/name order of a Kubernetes list.
        objects.sort_by(|a, b| {
            (&a.metadata.namespace, &a.metadata.name)
                .cmp(&(&b.metadata.namespace, &b.metadata.name))
        });
        Ok(objects.iter().map(|object| object.data.clone()).collect())
    }
}

fn start(client: &Client, kind: &'static str, plural: &str) -> Running {
    let mut resource = ApiResource::from_gvk(&GroupVersionKind::gvk("", "v1", kind));
    resource.plural = plural.to_string();
    let api = Api::<DynamicObject>::all_with(client.clone(), &resource);
    let mut writer = Writer::new(resource);
    let store = writer.as_reader();
    let (synced_tx, synced) = watch::channel(None);
    let task = tokio::spawn(async move {
        loop {
            let mut stream = watcher::watcher(api.clone(), watcher::Config::default()).boxed();
            while let Some(event) = stream.next().await {
                match event {
                    Ok(event) => {
                        let done = matches!(event, Event::InitDone);
                        writer.apply_watcher_event(&event.modify(|object| trim(kind, object)));
                        if done {
                            synced_tx.send_replace(Some(Ok(())));
                        }
                    }
                    Err(error) => {
                        warn!(kind, %error, "Kubernetes discovery watch failed; retrying");
                        synced_tx.send_replace(Some(Err(error.to_string())));
                        break;
                    }
                }
            }
            // The store keeps the last complete list until the relist finishes.
            if matches!(*synced_tx.borrow(), Some(Ok(()))) {
                synced_tx.send_replace(None);
            }
            time::sleep(RETRY_DELAY).await;
        }
    });
    Running {
        store,
        synced,
        _task: AbortOnDropHandle::new(task),
    }
}

/// Keep only what target discovery reads, in the shape it reads it.
fn trim(kind: &str, object: &mut DynamicObject) {
    let fields = &mut object.data;
    let mut trimmed = match kind {
        "Service" => {
            let ports = fields.pointer_mut("/spec/ports").map(Value::take);
            serde_json::json!({ "spec": { "ports": ports } })
        }
        "Pod" => {
            let containers = fields
                .pointer_mut("/spec/containers")
                .and_then(Value::as_array_mut);
            let ports: Vec<_> = containers
                .into_iter()
                .flatten()
                .map(|container| {
                    serde_json::json!({ "ports": container.get_mut("ports").map(Value::take) })
                })
                .collect();
            let ip = fields.pointer_mut("/status/podIP").map(Value::take);
            serde_json::json!({ "spec": { "containers": ports }, "status": { "podIP": ip } })
        }
        _ => {
            let subsets = fields.get_mut("subsets").map(Value::take);
            serde_json::json!({ "subsets": subsets })
        }
    };
    let metadata = std::mem::take(&mut object.metadata);
    trimmed["metadata"] = serde_json::json!({
        "namespace": metadata.namespace,
        "name": metadata.name,
        "labels": metadata.labels,
    });
    object.data = trimmed;
    object.metadata = ObjectMeta {
        namespace: metadata.namespace,
        name: metadata.name,
        ..ObjectMeta::default()
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trim_keeps_the_fields_the_old_list_discovery_kept() {
        let mut pod: DynamicObject = serde_json::from_value(json!({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": "api", "namespace": "apps", "labels": {"app": "api"},
                "annotations": {"big": "x"}, "uid": "u", "resourceVersion": "7",
                "managedFields": [{"manager": "kubectl"}]
            },
            "spec": {
                "nodeName": "node-a",
                "containers": [
                    {"name": "app", "image": "app:1", "env": [{"name": "A", "value": "b"}],
                     "ports": [{"name": "metrics", "containerPort": 8080}]},
                    {"name": "sidecar", "image": "sidecar:1"}
                ]
            },
            "status": {"podIP": "10.0.0.2", "phase": "Running", "conditions": []}
        }))
        .unwrap();
        trim("Pod", &mut pod);
        assert_eq!(
            pod.data,
            json!({
                "metadata": {"namespace": "apps", "name": "api", "labels": {"app": "api"}},
                "spec": {"containers": [
                    {"ports": [{"name": "metrics", "containerPort": 8080}]},
                    {"ports": null}
                ]},
                "status": {"podIP": "10.0.0.2"}
            })
        );
        assert_eq!(pod.metadata.name.as_deref(), Some("api"));
        assert!(pod.metadata.annotations.is_none() && pod.metadata.managed_fields.is_none());

        let mut service: DynamicObject = serde_json::from_value(json!({
            "metadata": {"name": "api", "namespace": "apps"},
            "spec": {"clusterIP": "10.1.0.1", "ports": [{"name": "metrics", "port": 9100}]}
        }))
        .unwrap();
        trim("Service", &mut service);
        assert_eq!(
            service.data,
            json!({
                "metadata": {"namespace": "apps", "name": "api", "labels": null},
                "spec": {"ports": [{"name": "metrics", "port": 9100}]}
            })
        );

        let mut endpoints: DynamicObject = serde_json::from_value(json!({
            "metadata": {"name": "api", "namespace": "apps", "annotations": {"a": "b"}},
            "subsets": [{"addresses": [{"ip": "10.0.0.1"}], "ports": [{"port": 9100}]}]
        }))
        .unwrap();
        trim("Endpoints", &mut endpoints);
        assert_eq!(
            endpoints.data,
            json!({
                "metadata": {"namespace": "apps", "name": "api", "labels": null},
                "subsets": [{"addresses": [{"ip": "10.0.0.1"}], "ports": [{"port": 9100}]}]
            })
        );
    }
}
