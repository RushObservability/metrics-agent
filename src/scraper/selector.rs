use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use serde_json::Value;

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct LabelSelector {
    match_labels: BTreeMap<String, String>,
    match_expressions: Vec<Requirement>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Requirement {
    key: String,
    operator: Operator,
    #[serde(default)]
    values: Vec<String>,
}

#[derive(Deserialize)]
enum Operator {
    In,
    NotIn,
    Exists,
    DoesNotExist,
}

impl LabelSelector {
    pub fn parse(value: Option<&Value>) -> Result<Self> {
        let Some(value) = value else {
            return Ok(Self::default());
        };
        let selector: Self =
            serde_json::from_value(value.clone()).context("invalid label selector")?;
        for (key, value) in &selector.match_labels {
            ensure!(
                valid_key(key) && valid_value(value),
                "invalid matchLabels requirement"
            );
        }
        for requirement in &selector.match_expressions {
            let has_values = !requirement.values.is_empty();
            ensure!(
                valid_key(&requirement.key)
                    && has_values == matches!(requirement.operator, Operator::In | Operator::NotIn)
                    && requirement.values.iter().all(|value| valid_value(value)),
                "invalid matchExpressions requirement"
            );
        }
        Ok(selector)
    }

    pub fn matches(&self, labels: Option<&Value>) -> bool {
        let label = |key: &str| {
            labels
                .and_then(|labels| labels.get(key))
                .and_then(Value::as_str)
        };
        self.match_labels
            .iter()
            .all(|(key, value)| label(key) == Some(value))
            && self.match_expressions.iter().all(|requirement| {
                let actual = label(&requirement.key);
                let in_set = || {
                    actual.is_some_and(|actual| {
                        requirement.values.iter().any(|value| value == actual)
                    })
                };
                match requirement.operator {
                    Operator::In => in_set(),
                    Operator::NotIn => !in_set(),
                    Operator::Exists => actual.is_some(),
                    Operator::DoesNotExist => actual.is_none(),
                }
            })
    }
}

fn valid_value(value: &str) -> bool {
    value.is_empty() || valid_label_name(value)
}

fn valid_label_name(name: &str) -> bool {
    name.len() <= 63
        && name
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && name
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

fn valid_key(key: &str) -> bool {
    match key.split_once('/') {
        None => valid_label_name(key),
        Some((prefix, name)) => {
            prefix.len() <= 253 && prefix.split('.').all(valid_namespace) && valid_label_name(name)
        }
    }
}

fn valid_namespace(name: &str) -> bool {
    let alphanumeric = |b: &u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    name.len() <= 63
        && name.as_bytes().first().is_some_and(alphanumeric)
        && name.as_bytes().last().is_some_and(alphanumeric)
        && name.bytes().all(|b| alphanumeric(&b) || b == b'-')
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields, rename_all = "camelCase")]
struct NamespaceSelector {
    any: bool,
    match_names: Vec<String>,
}

pub(super) fn selected_namespaces(
    value: Option<&Value>,
    own: &str,
    allowed: &[String],
) -> Result<Vec<String>> {
    let selector: NamespaceSelector = match value {
        Some(value) => {
            serde_json::from_value(value.clone()).context("invalid namespace selector")?
        }
        None => NamespaceSelector::default(),
    };
    ensure!(
        selector
            .match_names
            .iter()
            .all(|name| valid_namespace(name)),
        "invalid namespace in matchNames"
    );

    let allow_all = allowed.iter().any(|name| name.trim() == "*");
    let permits = |name: &str| {
        allow_all
            || if allowed.is_empty() {
                name == own
            } else {
                allowed.iter().any(|allowed| allowed.trim() == name)
            }
    };
    let mut selected = if selector.any {
        if allow_all {
            vec!["*".into()]
        } else if allowed.is_empty() {
            vec![own.to_string()]
        } else {
            allowed.iter().map(|name| name.trim().to_string()).collect()
        }
    } else if selector.match_names.is_empty() {
        vec![own.to_string()]
            .into_iter()
            .filter(|name| permits(name))
            .collect()
    } else {
        selector
            .match_names
            .into_iter()
            .filter(|name| permits(name))
            .collect()
    };
    selected.sort_unstable();
    selected.dedup();
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn kube_label_operators_are_anded_and_handle_missing_keys() {
        let selector = LabelSelector::parse(Some(&json!({
            "matchLabels":{"team":"payments"},
            "matchExpressions":[
                {"key":"tier","operator":"In","values":["api","db"]},
                {"key":"env","operator":"NotIn","values":["dev"]},
                {"key":"zone","operator":"Exists"},
                {"key":"paused","operator":"DoesNotExist"}
            ]
        })))
        .unwrap();
        for (labels, expected) in [
            (json!({"team":"payments","tier":"db","zone":""}), true),
            (
                json!({"team":"payments","tier":"db","zone":"","env":"prod"}),
                true,
            ),
            (
                json!({"team":"payments","tier":"db","zone":"","env":"dev"}),
                false,
            ),
            (
                json!({"team":"payments","tier":"db","zone":"","paused":""}),
                false,
            ),
            (json!({"team":"payments","tier":"db"}), false),
            (json!({"team":"payments","zone":""}), false),
            (json!({"team":"other","tier":"db","zone":""}), false),
        ] {
            assert_eq!(selector.matches(Some(&labels)), expected, "{labels}");
        }
    }

    #[test]
    fn rejects_unsupported_or_malformed_selectors() {
        for expression in [
            json!({"key":"tier","operator":"Gt","values":["1"]}),
            json!({"key":"tier","operator":"In"}),
            json!({"key":"tier","operator":"Exists","values":["api"]}),
            json!({"key":"#","operator":"NotIn","values":["api"]}),
        ] {
            assert!(LabelSelector::parse(Some(&json!({"matchExpressions":[expression]}))).is_err());
        }
        assert!(LabelSelector::parse(Some(&json!({"other":{}}))).is_err());
        assert!(selected_namespaces(Some(&json!({"matchNames":["*"]})), "monitor", &[]).is_err());
        assert!(selected_namespaces(Some(&json!({"matchLabels":{}})), "monitor", &[]).is_err());
    }

    #[test]
    fn namespace_policy_limits_any_and_explicit_cross_namespace_selections() {
        let any = json!({"any": true});
        let names = json!({"matchNames": ["ops", "apps"]});
        assert_eq!(
            selected_namespaces(Some(&any), "monitor", &[]).unwrap(),
            ["monitor"]
        );
        assert!(
            selected_namespaces(Some(&names), "monitor", &[])
                .unwrap()
                .is_empty()
        );
        for selector in [&any, &names] {
            assert_eq!(
                selected_namespaces(Some(selector), "monitor", &["apps".into()]).unwrap(),
                ["apps"]
            );
        }
        assert_eq!(
            selected_namespaces(Some(&any), "monitor", &["*".into()]).unwrap(),
            ["*"]
        );
    }
}
