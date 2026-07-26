use serde_json::{Value, json};

pub const IGNORE_PROMETHEUS_UPDATES_ANNOTATION: &str =
    "operator.victoriametrics.com/ignore-prometheus-updates";
pub const IGNORE_PROMETHEUS_UPDATES_ENABLED: &str = "enabled";
pub const PREFER_SOURCE_ANNOTATION: &str = "metrics-agent.rushobservability.com/prefer-source";
pub const PREFER_VICTORIA_METRICS: &str = "victoriametrics";
pub const PREFER_PROMETHEUS: &str = "prometheus";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PrecedenceInput<'a> {
    pub explicit_preference: Option<&'a str>,
    pub has_prometheus_owner: bool,
    pub prometheus_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrecedenceDecision {
    pub ignore_prometheus_updates: bool,
    pub reason: &'static str,
}

pub fn decide(input: PrecedenceInput<'_>) -> PrecedenceDecision {
    match input.explicit_preference {
        Some(PREFER_VICTORIA_METRICS) => PrecedenceDecision {
            ignore_prometheus_updates: true,
            reason: "explicit-victoriametrics-preference",
        },
        Some(PREFER_PROMETHEUS) => PrecedenceDecision {
            ignore_prometheus_updates: false,
            reason: "explicit-prometheus-preference",
        },
        _ if input.has_prometheus_owner => PrecedenceDecision {
            ignore_prometheus_updates: false,
            reason: "prometheus-converted-object",
        },
        _ if input.prometheus_exists => PrecedenceDecision {
            ignore_prometheus_updates: true,
            reason: "same-name-native-vm-object",
        },
        _ => PrecedenceDecision {
            ignore_prometheus_updates: true,
            reason: "native-vm-object",
        },
    }
}

pub fn has_prometheus_owner(
    owners: &[k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference],
    expected_kind: &str,
    expected_name: &str,
) -> bool {
    owners.iter().any(|owner| {
        owner.api_version.starts_with("monitoring.coreos.com/")
            && owner.kind == expected_kind
            && owner.name == expected_name
    })
}

pub fn filter_prometheus_owner(
    owners: &[k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference],
    expected_kind: &str,
    expected_name: &str,
) -> Vec<k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference> {
    owners
        .iter()
        .filter(|owner| {
            !(owner.api_version.starts_with("monitoring.coreos.com/")
                && owner.kind == expected_kind
                && owner.name == expected_name)
        })
        .cloned()
        .collect()
}

pub fn patch(
    decision: &PrecedenceDecision,
    replace_owners: bool,
    owners: &[k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference],
) -> Value {
    let annotation = if decision.ignore_prometheus_updates {
        json!(IGNORE_PROMETHEUS_UPDATES_ENABLED)
    } else {
        Value::Null
    };
    let mut metadata = json!({
        "annotations": {
            IGNORE_PROMETHEUS_UPDATES_ANNOTATION: annotation,
        }
    });
    if replace_owners {
        metadata["ownerReferences"] = serde_json::to_value(owners).unwrap_or_else(|_| json!([]));
    }
    json!({"metadata": metadata})
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

    fn owner(api_version: &str, kind: &str, name: &str) -> OwnerReference {
        OwnerReference {
            api_version: api_version.to_string(),
            kind: kind.to_string(),
            name: name.to_string(),
            uid: format!("uid-{name}"),
            block_owner_deletion: None,
            controller: None,
        }
    }

    #[test]
    fn explicit_source_precedes_owner_and_collision() {
        assert_eq!(
            decide(PrecedenceInput {
                explicit_preference: Some(PREFER_VICTORIA_METRICS),
                has_prometheus_owner: true,
                prometheus_exists: true,
            }),
            PrecedenceDecision {
                ignore_prometheus_updates: true,
                reason: "explicit-victoriametrics-preference",
            }
        );
        assert!(
            !decide(PrecedenceInput {
                explicit_preference: Some(PREFER_PROMETHEUS),
                has_prometheus_owner: false,
                prometheus_exists: true,
            })
            .ignore_prometheus_updates
        );
    }

    #[test]
    fn native_vm_objects_are_protected_before_collision() {
        assert!(decide(PrecedenceInput::default()).ignore_prometheus_updates);
        assert!(
            decide(PrecedenceInput {
                prometheus_exists: true,
                ..Default::default()
            })
            .ignore_prometheus_updates
        );
    }

    #[test]
    fn precedence_falls_back_from_unknown_source_annotation() {
        assert_eq!(
            decide(PrecedenceInput {
                explicit_preference: Some("unknown"),
                has_prometheus_owner: true,
                prometheus_exists: true,
            }),
            PrecedenceDecision {
                ignore_prometheus_updates: false,
                reason: "prometheus-converted-object",
            }
        );
        assert_eq!(
            decide(PrecedenceInput {
                explicit_preference: Some("unknown"),
                prometheus_exists: true,
                ..Default::default()
            })
            .reason,
            "same-name-native-vm-object"
        );
    }

    #[test]
    fn owner_reference_requires_prometheus_group_kind_and_name() {
        let owners = vec![
            owner("apps/v1", "ServiceMonitor", "payments"),
            owner("monitoring.coreos.com/v1", "PodMonitor", "payments"),
            owner("monitoring.coreos.com/v1", "ServiceMonitor", "checkout"),
            owner("monitoring.coreos.com/v1", "ServiceMonitor", "payments"),
        ];

        assert!(has_prometheus_owner(&owners, "ServiceMonitor", "payments"));
        assert!(!has_prometheus_owner(&owners, "ServiceMonitor", "missing"));
        assert!(has_prometheus_owner(&owners, "PodMonitor", "payments"));
        assert!(!has_prometheus_owner(&owners, "Probe", "payments"));

        let filtered = filter_prometheus_owner(&owners, "ServiceMonitor", "payments");
        assert_eq!(filtered.len(), 3);
        assert_eq!(filtered[0].api_version, "apps/v1");
        assert_eq!(filtered[1].kind, "PodMonitor");
        assert_eq!(filtered[2].name, "checkout");
    }

    #[test]
    fn patch_sets_or_removes_only_the_managed_annotation() {
        let victoria_patch = patch(
            &PrecedenceDecision {
                ignore_prometheus_updates: true,
                reason: "native-vm-object",
            },
            false,
            &[],
        );
        assert_eq!(
            victoria_patch["metadata"]["annotations"][IGNORE_PROMETHEUS_UPDATES_ANNOTATION],
            IGNORE_PROMETHEUS_UPDATES_ENABLED
        );
        assert!(victoria_patch["metadata"].get("ownerReferences").is_none());

        let prometheus_patch = patch(
            &PrecedenceDecision {
                ignore_prometheus_updates: false,
                reason: "prometheus-converted-object",
            },
            false,
            &[],
        );
        assert!(
            prometheus_patch["metadata"]["annotations"][IGNORE_PROMETHEUS_UPDATES_ANNOTATION]
                .is_null()
        );
        assert!(
            prometheus_patch["metadata"]
                .get("ownerReferences")
                .is_none()
        );
    }

    #[test]
    fn patch_replaces_owner_references_only_when_requested() {
        let owners = vec![owner("apps/v1", "Deployment", "collector")];
        let value = patch(
            &PrecedenceDecision {
                ignore_prometheus_updates: true,
                reason: "explicit-victoriametrics-preference",
            },
            true,
            &owners,
        );

        assert_eq!(value["metadata"]["ownerReferences"], json!(owners));
        assert_eq!(
            value["metadata"]["annotations"][IGNORE_PROMETHEUS_UPDATES_ANNOTATION],
            IGNORE_PROMETHEUS_UPDATES_ENABLED
        );
    }
}
