#!/usr/bin/env bash
set -euo pipefail
chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
enabled=(--set sreMetrics.enabled=true --set sreMetrics.url=http://sre-agent:8081/metrics --set sreMetrics.tokenSecret.name=agent-internal)
rendered="$(helm template health "$chart_dir" "${enabled[@]}" --set networkPolicy.enabled=true)"
for expected in 'name: METRICS_AGENT_SRE_METRICS_URL' 'value: "http://sre-agent:8081/metrics"' 'value: /var/run/rush-sre-metrics/token' 'secretName: "agent-internal"' 'key: "sre-agent-internal-token"' 'readOnly: true' 'app.kubernetes.io/component: sre-agent'; do
  grep -Fq "$expected" <<<"$rendered" || { echo "missing SRE metrics setting: $expected" >&2; exit 1; }
done
if grep -Fq 'subPath:' <<<"$rendered"; then
  echo 'token mount must support projected Secret rotation' >&2
  exit 1
fi
if grep -Fq 'METRICS_AGENT_SRE_METRICS' <<<"$(helm template health "$chart_dir")"; then
  echo 'SRE collection must remain opt-in' >&2
  exit 1
fi
if grep -Fq 'METRICS_AGENT_SRE_METRICS' <<<"$(helm template health "$chart_dir" "${enabled[@]}" --set scrape.enabled=false)"; then
  echo 'disabled scraping must not project SRE credentials' >&2
  exit 1
fi
if helm template health "$chart_dir" --set sreMetrics.enabled=true >/dev/null 2>&1; then
  echo 'incomplete SRE configuration was accepted' >&2
  exit 1
fi
fanout="$(helm template health "$chart_dir" "${enabled[@]}" --namespace monitoring --set sreMetrics.discovery.enabled=true --set sreMetrics.discovery.serviceName=sre-agent)"
for expected in 'name: METRICS_AGENT_SRE_METRICS_SERVICE' 'value: "sre-agent"' 'name: METRICS_AGENT_SRE_METRICS_NAMESPACE' 'fieldPath: metadata.namespace'; do
  grep -Fq "$expected" <<<"$fanout" || { echo "missing replica discovery setting: $expected" >&2; exit 1; }
done
echo 'standalone authenticated SRE metrics renders passed'
