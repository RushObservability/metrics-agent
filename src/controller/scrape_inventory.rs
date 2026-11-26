use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Result, anyhow};
use kube::api::{DynamicObject, ResourceExt};
use serde_json::Value;
use tokio::sync::RwLock;

use super::{ResourcePair, object_key};

pub(crate) struct ScrapeObject {
    pub namespace: String,
    pub name: String,
    pub spec: Value,
}

pub(super) type Objects = BTreeMap<String, Arc<ScrapeObject>>;

pub(crate) struct ScrapeInventory {
    allowed_namespaces: Vec<String>,
    // None marks a source with an incomplete list or a failed watch.
    pairs: RwLock<Vec<[Option<Objects>; 2]>>,
}

impl ScrapeInventory {
    pub fn new(pair_count: usize, allowed_namespaces: &[String]) -> Self {
        Self {
            allowed_namespaces: allowed_namespaces.to_vec(),
            pairs: RwLock::new(vec![[None, None]; pair_count]),
        }
    }

    pub fn allowed_namespaces(&self) -> &[String] {
        &self.allowed_namespaces
    }

    pub(super) fn insert(&self, objects: &mut Objects, object: &DynamicObject) {
        if let Some(selected) = self.select(object) {
            objects.insert(object_key(object), selected);
        }
    }

    fn select(&self, object: &DynamicObject) -> Option<Arc<ScrapeObject>> {
        let namespace = object.namespace().unwrap_or_else(|| "default".into());
        if !crate::scraper::namespace_allowed(&namespace, &self.allowed_namespaces) {
            return None;
        }
        Some(Arc::new(ScrapeObject {
            namespace,
            name: object.name_any(),
            spec: object.data.get("spec").cloned().unwrap_or(Value::Null),
        }))
    }

    pub(super) async fn replace(&self, pair: usize, prometheus: bool, objects: Option<Objects>) {
        self.pairs.write().await[pair][usize::from(prometheus)] = objects;
    }

    pub(super) async fn apply(&self, pair: usize, prometheus: bool, object: &DynamicObject) {
        let Some(selected) = self.select(object) else {
            return;
        };
        if let Some(objects) = &mut self.pairs.write().await[pair][usize::from(prometheus)] {
            objects.insert(object_key(object), selected);
        }
    }

    pub(super) async fn delete(&self, pair: usize, prometheus: bool, object: &DynamicObject) {
        if let Some(objects) = &mut self.pairs.write().await[pair][usize::from(prometheus)] {
            objects.remove(&object_key(object));
        }
    }

    pub async fn selected(
        &self,
        pairs: &[ResourcePair],
    ) -> Result<Vec<(&'static str, &'static str, Arc<ScrapeObject>)>> {
        let inventory = self.pairs.read().await;
        let mut selected = Vec::new();
        for (pair, [victoria, prometheus]) in pairs.iter().zip(inventory.iter()) {
            let victoria = victoria
                .as_ref()
                .ok_or_else(|| anyhow!("{} VictoriaMetrics watch is unavailable", pair.name))?;
            let prometheus = prometheus
                .as_ref()
                .ok_or_else(|| anyhow!("{} Prometheus watch is unavailable", pair.name))?;
            selected.extend(
                victoria
                    .values()
                    .map(|object| (pair.name, "victoria", Arc::clone(object))),
            );
            selected.extend(
                prometheus
                    .iter()
                    .filter(|(key, _)| !victoria.contains_key(*key))
                    .map(|(_, object)| (pair.name, "prometheus", Arc::clone(object))),
            );
        }
        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config(namespace: &str, target: &str) -> DynamicObject {
        serde_json::from_value(json!({
            "metadata": { "namespace": namespace, "name": "config" },
            "spec": { "staticConfigs": [{ "targets": [target] }] }
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn selects_allowed_sources_and_preserves_victoria_precedence_across_updates() {
        let pairs = vec![super::super::resource_pairs().pop().unwrap()];
        let inventory = ScrapeInventory::new(pairs.len(), &["monitoring".into()]);
        let victoria = config("monitoring", "victoria:9100");
        let prometheus = config("monitoring", "prometheus:9100");
        let denied = config("other", "other:9100");

        let mut initial = Objects::new();
        inventory.insert(&mut initial, &victoria);
        inventory.insert(&mut initial, &denied);
        inventory.replace(0, false, Some(initial)).await;
        inventory.replace(0, true, Some(Objects::new())).await;
        inventory.apply(0, true, &prometheus).await;

        let selected = inventory.selected(&pairs).await.unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].0, "scrape-config");
        assert_eq!(selected[0].1, "victoria");
        assert_eq!(
            selected[0].2.spec["staticConfigs"][0]["targets"][0],
            "victoria:9100"
        );

        inventory.delete(0, false, &victoria).await;
        let selected = inventory.selected(&pairs).await.unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].1, "prometheus");

        inventory.replace(0, true, None).await;
        assert!(inventory.selected(&pairs).await.is_err());
        inventory.replace(0, true, Some(Objects::new())).await;
        assert!(inventory.selected(&pairs).await.unwrap().is_empty());
    }
}
