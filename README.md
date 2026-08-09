<div align="center">

# metrics-agent

**Discover the scrape config. Preserve the right source. Ship the signal.**

[![ci](https://github.com/RushObservability/metrics-agent/actions/workflows/ci.yml/badge.svg)](https://github.com/RushObservability/metrics-agent/actions/workflows/ci.yml)
[![release](https://github.com/RushObservability/metrics-agent/actions/workflows/release.yml/badge.svg)](https://github.com/RushObservability/metrics-agent/actions/workflows/release.yml)

</div>

metrics-agent is a Kubernetes-native metrics collector for Rush. It watches
Prometheus Operator and VictoriaMetrics scrape resources, resolves collisions
between converted and native objects, scrapes discovered endpoints, and
publishes samples directly to query-api through Prometheus remote write.

It also includes a small control surface for collection health, process resource
usage, discovered CRDs, scrape status, and per-CRD metric cardinality.

> metrics-agent is normally deployed as part of a [Rush](https://github.com/RushObservability)
> installation. It publishes workload metrics directly to Rush.

![metrics-agent control surface](docs/metrics-agent-ui.jpg)

The embedded control surface gives operators a live view of CRD discovery,
collection health, process resource usage, and Rush publishing.

## Why use metrics-agent?

Use metrics-agent when you want to discover and collect the scrape configuration
already present in a Kubernetes cluster without installing a separate datasource.
It understands Prometheus Operator and VictoriaMetrics Operator CRDs, resolves
their targets, and sends the resulting
metrics directly to Rush.

This is useful when you want to:

- inspect which Services, Pods, probes, and scrape configs are configured;
- preserve native VictoriaMetrics configurations alongside Prometheus resources;
- avoid deploying another metrics datasource just to collect existing targets;
- send workload and collector-health metrics directly to a Rush tenant; and
- see collection health, cardinality, and resource usage from the embedded UI.

## What it does

**Discover.** Watches supported scrape CRDs and the Kubernetes Services,
Endpoints, and Pods needed to resolve their targets.

**Preserve precedence.** Keeps native VictoriaMetrics scrape objects from being
overwritten by the Prometheus converter, while allowing converter-owned objects
to follow their Prometheus source.

**Collect.** Scrapes Prometheus exposition endpoints directly from the Rust
process with bounded concurrency and bounded remote-write batches.

**Publish.** Sends workload samples and agent health metrics to Rush's
Prometheus-compatible remote-write endpoint with optional tenant and bearer
token routing.

**Inspect.** Serves /livez, /readyz, /metrics, JSON status endpoints, and an
embedded UI at /ui/ by default.

## How it works

    Kubernetes API
          │
          ├── Prometheus scrape CRDs ──┐
          └── VictoriaMetrics CRDs ───┤
                                       ▼
                             precedence controller
                                       │
                             target discovery + scrape
                                       │
                                       ▼
                             Prometheus remote write
                                       │
                                       ▼
                                  query-api → Rush

The agent keeps the scrape pipeline in memory and streams samples in bounded
batches. It does not retain an entire scrape cycle before publishing it.

## Supported resources

| Prometheus Operator | VictoriaMetrics Operator |
| --- | --- |
| ServiceMonitor | VMServiceScrape |
| PodMonitor | VMPodScrape |
| Probe | VMProbe |
| ScrapeConfig | VMScrapeConfig |

Rule and Alertmanager resources are excluded because they do not define scrape
targets.

## Precedence

VictoriaMetrics Operator-created objects carry an owner reference to their
same-name Prometheus object. metrics-agent uses that reference to distinguish
converter-owned objects from native VictoriaMetrics objects:

1. Converter-owned VM objects continue to follow Prometheus configuration.
2. Native VM objects receive the
   operator.victoriametrics.com/ignore-prometheus-updates: enabled annotation.
3. A native VM object wins when Prometheus and VictoriaMetrics resources share
   a name and namespace.

For an exceptional migration, force a source with an annotation:

    metadata:
      annotations:
        metrics-agent.rushobservability.com/prefer-source: victoriametrics

The other accepted value is prometheus. Forcing victoriametrics also removes
the matching Prometheus owner reference so the object cannot be garbage-collected
with the Prometheus resource.

## Requirements

- Kubernetes 1.27 or newer
- Helm 3
- Prometheus Operator CRDs when Prometheus scrape resources are used
- VictoriaMetrics Operator CRDs when VictoriaMetrics scrape resources are used

Configure the VictoriaMetrics converter with owner references enabled:

    operator:
      disable_prometheus_converter: false
      enable_converter_ownership: true

The operator Deployment must have
VM_ENABLEDPROMETHEUSCONVERTEROWNERREFERENCES=true so the controller can identify
converted objects reliably.

## Quick start

Run the local test and verification gates:

    make test
    make verify

Run against the current Kubernetes context and publish to a local query-api on
port 8080:

    make run

The local default is:

    remote write: http://localhost:8080/prom/api/v1/write
    tenant:       default
    HTTP/UI:      :7070
    UI path:      /ui/

Override the destination when needed:

    make run \
      RUSH_REMOTE_WRITE_URL=http://localhost:8080/prom/api/v1/write \
      RUSH_REMOTE_WRITE_TENANT=default

Use METRICS_AGENT_KUBECONFIG=/path/to/config when the active kubeconfig is not
the one to inspect.

## Install with Helm

Build and publish the image, or use a release image from GHCR:

    helm upgrade --install metrics-agent ./helm-chart \
      --namespace monitoring \
      --create-namespace \
      --set image.repository=ghcr.io/rushobservability/metrics-agent \
      --set image.tag=0.1.0 \
      --set rushRemoteWrite.enabled=true \
      --set rushRemoteWrite.url=http://rush-query-api.monitoring.svc.cluster.local:8080/prom/api/v1/write

For secured Rush ingestion, create an **ingest-only** API key scoped to the
target tenant and the `metrics` signal. Keep that bearer token in a Kubernetes
Secret rather than in Helm values:

    kubectl -n monitoring create secret generic rush-remote-write \
      --from-literal=token=your_tenant_scoped_api_key

    rushRemoteWrite:
      enabled: true
      url: http://rush-query-api.monitoring.svc.cluster.local:8080/prom/api/v1/write
      bearerTokenSecret:
        name: rush-remote-write
        key: token

For a tenant whose **Require ingest key** setting is off, omit the Secret and
set `rushRemoteWrite.allowAnonymous: true`. Anonymous remote write is otherwise
rejected by the chart and by Rush.

The final image uses a Chainguard Rust builder and glibc-dynamic runtime. It
runs as UID 65532, has no shell or package manager, and uses a read-only
filesystem with Linux capabilities dropped.

For a security-focused installation, start with
examples/values-secure.yaml. Pin image.digest to a release digest and replace
the example NetworkPolicy egress rules with the exact Kubernetes API-server,
DNS, scrape-target, and Rush/query-api destinations for your cluster.

The chart deploys exactly one metrics-agent replica. Values other than
replicaCount: 1 are rejected until the controller supports leader election.

## Configuration

CLI flags and environment variables use the same names. The most commonly used
settings are:

| Variable | Default | Purpose |
| --- | --- | --- |
| METRICS_AGENT_HTTP_ADDRESS | :7070 | Main HTTP listener |
| METRICS_AGENT_KUBECONFIG | unset | Explicit kubeconfig path |
| METRICS_AGENT_RESYNC_PERIOD | 5m | Full CRD resync interval |
| METRICS_AGENT_WORKERS | 2 | Reconciliation workers |
| METRICS_AGENT_LOG_LEVEL | info | Log level |
| METRICS_AGENT_UI_ENABLED | true | Enable the embedded UI |
| METRICS_AGENT_UI_ADDRESS | :7070 | UI listener; same listener by default |
| METRICS_AGENT_UI_PATH | /ui/ | UI mount path |
| RUSH_REMOTE_WRITE_URL | unset | Rush remote-write endpoint |
| RUSH_REMOTE_WRITE_INTERVAL | 15s | Self-metrics heartbeat interval |
| RUSH_REMOTE_WRITE_TOKEN | unset | Ingest-only key scoped to `metrics`; omit only for explicitly open ingestion |
| RUSH_REMOTE_WRITE_TENANT | unset | Optional routing hint; never grants access without a matching key |
| METRICS_AGENT_SCRAPE_ENABLED | true | Enable discovered-target scraping |
| METRICS_AGENT_SCRAPE_INTERVAL | 15s | Scrape cycle interval |
| METRICS_AGENT_SCRAPE_TIMEOUT | 10s | Per-target HTTP timeout |
| METRICS_AGENT_VERSION | dev | Version shown in status and UI |

The Helm chart exposes these through helm-chart/values.yaml, including UI, security
context, resources, ServiceMonitor, NetworkPolicy, and direct Rush remote-write
settings.

## Embedded control surface

| Endpoint | Purpose |
| --- | --- |
| GET /livez | Process liveness |
| GET /readyz | Informer/cache readiness |
| GET /metrics | Agent Prometheus metrics |
| GET /api/v1/status | Collection, process, CRD, cardinality, and remote-write status |
| GET /api/v1/metrics-summary | Status plus metric examples |
| GET /ui/ | Embedded control-room UI |

Port-forward a deployed agent to inspect it locally:

    kubectl -n monitoring port-forward svc/metrics-agent 7070:7070
    open http://localhost:7070/ui/
    curl http://localhost:7070/api/v1/status | jq

The UI shows process CPU and resident memory, watched CRD objects, target
health, collected series, per-CRD memory estimates, and the top metric names by
cardinality. Per-object memory is an attribution estimate of serialized
watch-cache data, not an independent operating-system allocation.

## Remote write to Rush

The agent sends Snappy-compressed Prometheus remote-write requests to:

    POST <rush-query-api>/prom/api/v1/write

For an open tenant, set the tenant in Helm values or use the URL form:

    rushRemoteWrite:
      enabled: true
      url: http://rush-query-api.monitoring.svc.cluster.local:8080/prom/api/v1/write
      tenant: my-team

The equivalent URL is:

    http://rush-query-api.monitoring.svc.cluster.local:8080/t/my-team/prom/api/v1/write

For a locked tenant, use a tenant-scoped API key as a bearer token. The token's
tenant is authoritative when both a token and tenant header are present.

Verify publishing from the agent status endpoint:

    curl http://localhost:7070/api/v1/status | jq .remote_write

Then query Rush through query-api:

    curl -G http://localhost:8080/prom/api/v1/query \
      -H 'Authorization: Bearer your_tenant_scoped_api_key' \
      --data-urlencode 'query=up'

## Operations and security

- The controller reads supported scrape CRDs and patches only the managed
  VictoriaMetrics precedence annotation and, when explicitly requested, the
  matching owner reference.
- The default ServiceAccount uses only the cluster-scoped read/watch permissions
  required for scrape discovery plus patch access to the supported VictoriaMetrics
  scrape resources; it has no Events write permission.
- The container runs non-root with a read-only filesystem and all Linux
  capabilities dropped, privilege escalation disabled, RuntimeDefault seccomp,
  and host namespaces disabled.
- Store remote-write tokens in Kubernetes Secrets or another secret manager.
- NetworkPolicy is configurable and empty ingress/egress lists are deny-all;
  enable it with cluster-specific rules for production deployments.
- Inspect /readyz, /metrics, and .remote_write in /api/v1/status when collection
  or delivery is degraded.

## Development

Useful local gates:

    make test
    make fmt
    make clippy
    make verify
    node --check ui/app.js

Build a local image:

    make image VERSION=0.1.0

The pull-request CI workflow runs formatting, all-feature Rust tests, Clippy,
UI syntax checks, Helm lint/render checks, and workflow YAML validation.

## Releases

Pushing a semantic-version tag creates a GitHub Release with Linux amd64 and
arm64 archives plus SHA-256 checksums. The same workflow publishes a
multi-architecture image to GHCR. helm- tags are ignored.

    git tag -a v0.1.0 -m metrics-agent-v0.1.0
    git push origin v0.1.0

Release assets are named like:

    metrics-agent-0.1.0-amd64.tar.gz
    metrics-agent-0.1.0-arm64.tar.gz

Images are published as:

    ghcr.io/rushobservability/metrics-agent:0.1.0
    ghcr.io/rushobservability/metrics-agent:v0.1.0

## Part of Rush

metrics-agent is normally deployed alongside:

- [query-api](https://github.com/RushObservability/query-api) — ingest, query, and tenant routing
- [sre-agent](https://github.com/RushObservability/sre-agent) — AI-assisted investigations
- [helm-charts](https://github.com/RushObservability/helm-charts) — shared deployment charts

## License

The project is licensed under Apache License 2.0. See [LICENSE](LICENSE) for
the complete terms; the Rust package and Helm chart metadata use Apache-2.0.
