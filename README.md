<div align="center">

# metrics-agent

**Kubernetes scrape discovery and remote write for [Rush](https://github.com/RushObservability).**

[![ci](https://github.com/RushObservability/metrics-agent/actions/workflows/ci.yml/badge.svg)](https://github.com/RushObservability/metrics-agent/actions/workflows/ci.yml)
[![release](https://github.com/RushObservability/metrics-agent/actions/workflows/release.yml/badge.svg)](https://github.com/RushObservability/metrics-agent/actions/workflows/release.yml)
![license](https://img.shields.io/badge/license-Apache--2.0-blue)

</div>

metrics-agent is the Rust service that turns Prometheus Operator and
VictoriaMetrics Operator scrape resources into metrics in Rush. It watches the
Kubernetes API, resolves Services and Pods into scrape targets, reads
Prometheus exposition, and sends the samples to query-api with Prometheus
remote write.

The two operators can describe the same target. That overlap is the awkward
part. metrics-agent keeps native VictoriaMetrics resources in control while
allowing converter-owned resources to follow their Prometheus source. It does
the reconciliation and collection in one process, without deploying another
Prometheus-compatible database.

## What it does

**Reconciles scrape resources.** The controller watches `ServiceMonitor`,
`PodMonitor`, `Probe`, and `ScrapeConfig` alongside their VictoriaMetrics
equivalents. It marks native VictoriaMetrics objects so the operator's
Prometheus converter does not overwrite them.

**Discovers and scrapes targets.** Services, Endpoints, and Pods are resolved
into HTTP targets. Scrapes run with fixed limits for concurrency, response
bytes, line length, labels, and samples. One bad exporter fails its own scrape;
it does not consume an unbounded amount of agent memory.

**Writes directly to Rush.** Workload samples and the agent's own health
metrics are sent to query-api in Snappy-compressed remote-write requests. The
agent streams bounded batches instead of holding a complete scrape cycle in
memory.

**Reports what it is doing.** Liveness, readiness, and Prometheus metrics are
always available. An opt-in local UI adds target health, CRD inventory,
cardinality, process use, and delivery status.

## Quick start

Run the tests, then start the agent against your current Kubernetes context.
query-api must be listening on `localhost:8080`.

```bash
make test
make run
```

`make run` uses these local defaults:

```text
Remote write  http://localhost:8080/prom/api/v1/write
Tenant        default
HTTP          http://localhost:7070
UI            disabled
```

Use a different kubeconfig or Rush endpoint when needed:

```bash
make run \
  METRICS_AGENT_KUBECONFIG=/path/to/kubeconfig \
  RUSH_REMOTE_WRITE_URL=http://localhost:8080/prom/api/v1/write \
  RUSH_REMOTE_WRITE_TENANT=default
```

The process needs permission to list and watch the supported scrape resources,
Services, Endpoints, and Pods. It also needs patch access to the supported
VictoriaMetrics scrape resources when precedence reconciliation is enabled.

## Install with Helm

Install the chart from this repository with a published image:

```bash
helm upgrade --install metrics-agent ./helm-chart \
  --namespace monitoring \
  --create-namespace \
  --set image.repository=ghcr.io/rushobservability/metrics-agent \
  --set image.tag=0.1.0 \
  --set rushRemoteWrite.enabled=true \
  --set rushRemoteWrite.url=http://rush-query-api.monitoring.svc.cluster.local:8080/prom/api/v1/write
```

For a locked Rush tenant, create an ingest-only API key with the `metrics`
signal. Put it in a Secret rather than a Helm value:

```bash
kubectl -n monitoring create secret generic rush-remote-write \
  --from-literal=token="${RUSH_INGEST_API_KEY}"
```

Reference the Secret in your values file:

```yaml
rushRemoteWrite:
  enabled: true
  url: http://rush-query-api.monitoring.svc.cluster.local:8080/prom/api/v1/write
  bearerTokenSecret:
    name: rush-remote-write
    key: token
```

If the tenant has **Require ingest key** turned off, omit the Secret and set
`rushRemoteWrite.allowAnonymous: true`. The chart rejects an unauthenticated
configuration unless that value is explicit.

Add labels to every outgoing series with `extraLabels`. These values replace
same-named labels supplied by targets:

```yaml
extraLabels:
  env: dev
  cluster: ntt-japan
```

The chart supports one replica. It rejects larger values until the controller
has leader election. For a locked-down starting point, use
[`examples/values-secure.yaml`](examples/values-secure.yaml) and replace its
example NetworkPolicy rules with the addresses used by your cluster.

## Scrape resources and precedence

| Prometheus Operator | VictoriaMetrics Operator |
| --- | --- |
| `ServiceMonitor` | `VMServiceScrape` |
| `PodMonitor` | `VMPodScrape` |
| `Probe` | `VMProbe` |
| `ScrapeConfig` | `VMScrapeConfig` |

Rule and Alertmanager resources are not watched because they do not define
scrape targets.

VictoriaMetrics objects created by the Prometheus converter carry an owner
reference to their source object. metrics-agent uses that reference to make the
choice:

1. Converter-owned VictoriaMetrics objects continue to follow Prometheus.
2. Native VictoriaMetrics objects receive
   `operator.victoriametrics.com/ignore-prometheus-updates: enabled`.
3. A native object wins when both sources use the same name and namespace.

The VictoriaMetrics Operator must create those owner references:

```yaml
operator:
  disable_prometheus_converter: false
  enable_converter_ownership: true
```

The corresponding Deployment variable is
`VM_ENABLEDPROMETHEUSCONVERTEROWNERREFERENCES=true`.

During a migration, an annotation can force either source:

```yaml
metadata:
  annotations:
    metrics-agent.rushobservability.com/prefer-source: victoriametrics
```

The other accepted value is `prometheus`. Choosing `victoriametrics` also
removes the matching Prometheus owner reference so Kubernetes does not delete
the object with its former source.

## Configuration

Every setting has a CLI flag and an environment-variable form. Run
`metrics-agent --help` for the flags.

| Variable | Default | Purpose |
| --- | --- | --- |
| `METRICS_AGENT_HTTP_ADDRESS` | `:7070` | Liveness, readiness, and metrics listener |
| `METRICS_AGENT_KUBECONFIG` | unset | Explicit kubeconfig path |
| `METRICS_AGENT_RESYNC_PERIOD` | `5m` | Full CRD resync interval |
| `METRICS_AGENT_WORKERS` | `2` | Reconciliation workers |
| `METRICS_AGENT_LOG_LEVEL` | `info` | Agent log level |
| `METRICS_AGENT_UI_ENABLED` | `false` | Enable the UI and detailed status APIs |
| `METRICS_AGENT_UI_ADDRESS` | `:7070` | UI listener |
| `METRICS_AGENT_UI_PATH` | `/ui/` | UI mount path |
| `RUSH_REMOTE_WRITE_URL` | unset | query-api remote-write endpoint |
| `RUSH_REMOTE_WRITE_INTERVAL` | `15s` | Self-metrics heartbeat interval |
| `RUSH_REMOTE_WRITE_TIMEOUT` | `30s` | Remote-write request timeout |
| `RUSH_REMOTE_WRITE_CONNECT_TIMEOUT` | `5s` | Rush connection timeout |
| `RUSH_REMOTE_WRITE_TOKEN` | unset | Ingest-only API key scoped to `metrics` |
| `RUSH_REMOTE_WRITE_TENANT` | unset | Tenant routing hint; it does not grant access |
| `METRICS_AGENT_EXTRA_LABELS` | `{}` | JSON object merged into every outgoing series |
| `METRICS_AGENT_SCRAPE_ENABLED` | `true` | Enable workload scraping |
| `METRICS_AGENT_SCRAPE_INTERVAL` | `15s` | Scrape cycle interval |
| `METRICS_AGENT_SCRAPE_TIMEOUT` | `10s` | Per-target HTTP timeout |
| `METRICS_AGENT_SCRAPE_DISCOVERY_REFRESH_INTERVAL` | `60s` | Maximum age of the target cache |
| `METRICS_AGENT_SCRAPE_CONCURRENCY` | `8` | Concurrent target limit |
| `METRICS_AGENT_SCRAPE_ALLOWED_NAMESPACES` | empty | Comma-separated namespaces allowed to define targets |
| `METRICS_AGENT_SCRAPE_ALLOWED_DESTINATIONS` | empty | Exact hosts, IPs, or `*.suffix` exceptions to destination blocking |
| `METRICS_AGENT_SCRAPE_MAX_RESPONSE_BYTES` | `4194304` | Response-body limit per target |
| `METRICS_AGENT_SCRAPE_MAX_SAMPLES_PER_TARGET` | `50000` | Parsed sample limit per target |
| `METRICS_AGENT_SCRAPE_MAX_LABELS_PER_SAMPLE` | `64` | Merged label limit per sample |
| `METRICS_AGENT_SCRAPE_MAX_LABEL_NAME_BYTES` | `256` | Label-name byte limit |
| `METRICS_AGENT_SCRAPE_MAX_LABEL_VALUE_BYTES` | `4096` | Label-value and HELP-text byte limit |
| `METRICS_AGENT_SCRAPE_MAX_METRIC_NAME_BYTES` | `1024` | Metric-name byte limit |
| `METRICS_AGENT_SCRAPE_MAX_LINE_BYTES` | `65536` | Exposition-line byte limit |
| `METRICS_AGENT_VERSION` | `dev` | Version reported by the status API and UI |

The Helm equivalents live in [`helm-chart/values.yaml`](helm-chart/values.yaml).

## Remote write to Rush

The agent writes to query-api's Prometheus endpoint:

```text
POST <query-api>/prom/api/v1/write
```

An API key is authoritative for its tenant. `RUSH_REMOTE_WRITE_TENANT` and the
tenant path are routing hints; neither can move a key into another tenant. For
an open tenant, either of these forms works:

```text
http://rush-query-api.monitoring.svc.cluster.local:8080/prom/api/v1/write
http://rush-query-api.monitoring.svc.cluster.local:8080/t/my-team/prom/api/v1/write
```

When the detailed status endpoints are enabled, check the latest delivery:

```bash
curl http://localhost:7070/api/v1/status | jq .remote_write
```

Set `RUSH_API_KEY` to a query-capable key when checking the stored metrics:

```bash
curl -G http://localhost:8080/prom/api/v1/query \
  -H "Authorization: Bearer ${RUSH_API_KEY}" \
  --data-urlencode 'query=up'
```

## Status and local UI

| Endpoint | Available by default | Returns |
| --- | --- | --- |
| `GET /livez` | yes | Process liveness |
| `GET /readyz` | yes | Kubernetes watcher readiness |
| `GET /metrics` | yes | Agent Prometheus metrics |
| `GET /api/v1/status` | no | Scrape, CRD, process, cardinality, and delivery state |
| `GET /api/v1/metrics-summary` | no | Status plus metric examples |
| `GET /ui/` | no | Embedded operator UI |

The last three endpoints return `404` unless
`METRICS_AGENT_UI_ENABLED=true`. Keep them behind a port-forward or restricted
ingress because they include cluster inventory and metric names.

```bash
kubectl -n monitoring port-forward svc/metrics-agent 7070:7070
open http://localhost:7070/ui/
curl http://localhost:7070/api/v1/status | jq
```

![metrics-agent status UI](docs/metrics-agent-ui.jpg)

Per-object memory in the UI estimates serialized watch-cache data. It is not an
operating-system allocation for that object.

## Security and failure limits

- The default ServiceAccount can read the resources used for discovery and
  patch supported VictoriaMetrics scrape resources. It cannot write Events.
- Redirects are disabled. The resolver blocks loopback, link-local, cloud
  metadata, and Kubernetes API destinations, including DNS names that resolve
  to a blocked address. `METRICS_AGENT_SCRAPE_ALLOWED_DESTINATIONS` is the
  explicit escape hatch.
- A target that exceeds a response, line, label, metric-name, or sample limit
  fails before the agent publishes a partial payload.
- The published container runs as UID `65532` with a read-only filesystem,
  dropped capabilities, disabled privilege escalation, and RuntimeDefault
  seccomp.
- The optional NetworkPolicy is deny-all when enabled with empty rule lists.
  Supply the Kubernetes API, DNS, scrape target, and query-api egress rules for
  your cluster.

If collection stalls, check `/readyz`, then inspect `.scrape` and
`.remote_write` in `/api/v1/status`. A `401` or `403` from query-api usually
means the key is not an ingest key for the tenant and `metrics` signal. Zero
targets usually points to missing CRDs, selectors that match no Services or
Pods, or a namespace restriction.

## Development and releases

Build and run the local checks:

```bash
make build
make test
make clippy
make helm-lint
make helm-template
node --check ui/app.js
```

Build a local image with `make image VERSION=0.1.0`.

A semantic-version tag publishes Linux amd64 and arm64 archives, SHA-256
checksums, and a multi-architecture image in GHCR:

```bash
git tag -a v0.1.0 -m metrics-agent-v0.1.0
git push origin v0.1.0
```

Release images use both `0.1.0` and `v0.1.0` tags under
`ghcr.io/rushobservability/metrics-agent`.

## Part of Rush

metrics-agent can discover and scrape targets on its own, but Rush is the
destination it is built for. It normally runs alongside:

- [query-api](https://github.com/RushObservability/query-api), which accepts
  remote write and stores the samples
- [frontend](https://github.com/RushObservability/frontend), which queries and
  graphs the metrics
- [sre-agent](https://github.com/RushObservability/sre-agent), which uses them
  during investigations
- [helm-charts](https://github.com/RushObservability/helm-charts), which deploys
  the complete stack

## License

[Apache License 2.0](LICENSE).
