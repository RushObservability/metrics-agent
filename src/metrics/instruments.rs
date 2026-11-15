//! Small process-local instruments. Callers supply only fixed names and enum labels.
use std::{collections::BTreeMap, fmt::Write, sync::Mutex};

const BOUNDS: [f64; 10] = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0, 120.0];
type Labels = Vec<(&'static str, &'static str)>;
type Key = (&'static str, Labels);
#[derive(Default)]
pub(crate) struct Registry(Mutex<BTreeMap<Key, Value>>);
enum Value {
    Counter(u64),
    #[allow(dead_code)]
    Gauge(f64),
    Histogram {
        buckets: [u64; 10],
        count: u64,
        sum: f64,
    },
}
impl Registry {
    pub fn counter(
        &self,
        name: &'static str,
        labels: &[(&'static str, &'static str)],
        amount: u64,
    ) {
        let mut values = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Value::Counter(value) = values
            .entry((name, labels.to_vec()))
            .or_insert(Value::Counter(0))
        {
            *value = value.saturating_add(amount);
        }
    }
    #[allow(dead_code)]
    pub fn gauge(&self, name: &'static str, labels: &[(&'static str, &'static str)], value: f64) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((name, labels.to_vec()), Value::Gauge(value));
    }
    pub fn add_gauge(
        &self,
        name: &'static str,
        labels: &[(&'static str, &'static str)],
        amount: f64,
    ) {
        let mut values = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Value::Gauge(value) = values
            .entry((name, labels.to_vec()))
            .or_insert(Value::Gauge(0.0))
        {
            *value += amount;
        }
    }
    pub fn observe(&self, name: &'static str, labels: &[(&'static str, &'static str)], value: f64) {
        let mut values = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Value::Histogram {
            buckets,
            count,
            sum,
        } = values
            .entry((name, labels.to_vec()))
            .or_insert(Value::Histogram {
                buckets: [0; 10],
                count: 0,
                sum: 0.0,
            })
        {
            for (bucket, bound) in buckets.iter_mut().zip(BOUNDS) {
                if value <= bound {
                    *bucket += 1;
                }
            }
            *count += 1;
            *sum += value;
        }
    }
    pub fn render(&self) -> String {
        let values = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let mut output = String::new();
        let mut last_name = "";
        for ((name, labels), value) in values.iter() {
            if last_name != *name {
                let kind = match value {
                    Value::Counter(_) => "counter",
                    Value::Gauge(_) => "gauge",
                    Value::Histogram { .. } => "histogram",
                };
                let _ = writeln!(
                    output,
                    "# HELP {name} {}\n# TYPE {name} {kind}",
                    name.replace('_', " ")
                );
                last_name = name;
            }
            let labels = labels
                .iter()
                .map(|(k, v)| format!("{k}=\"{v}\""))
                .collect::<Vec<_>>()
                .join(",");
            let suffix = if labels.is_empty() {
                String::new()
            } else {
                format!("{{{labels}}}")
            };
            match value {
                Value::Counter(value) => {
                    let _ = writeln!(output, "{name}{suffix} {value}");
                }
                Value::Gauge(value) => {
                    let _ = writeln!(output, "{name}{suffix} {value}");
                }
                Value::Histogram {
                    buckets,
                    count,
                    sum,
                } => {
                    let prefix = if labels.is_empty() {
                        String::new()
                    } else {
                        format!("{labels},")
                    };
                    for (bound, count) in BOUNDS.iter().zip(buckets) {
                        let _ = writeln!(output, "{name}_bucket{{{prefix}le=\"{bound}\"}} {count}");
                    }
                    let _ = writeln!(
                        output,
                        "{name}_bucket{{{prefix}le=\"+Inf\"}} {count}\n{name}_sum{suffix} {sum}\n{name}_count{suffix} {count}"
                    );
                }
            }
        }
        output
    }
}
