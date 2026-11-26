#!/usr/bin/env bash
set -euo pipefail
chart_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
vap=(--api-versions admissionregistration.k8s.io/v1/ValidatingAdmissionPolicy)
rendered="$(helm template health "$chart_dir" --namespace monitoring "${vap[@]}")"
for expected in 'kind: ValidatingAdmissionPolicy' 'kind: ValidatingAdmissionPolicyBinding' 'validationActions: [Deny]' 'operations: ["UPDATE"]' 'resources: ["vmservicescrapes", "vmpodscrapes", "vmprobes", "vmscrapeconfigs"]' "request.userInfo.username == 'system:serviceaccount:monitoring:health-metrics-agent'" 'object.?spec == oldObject.?spec' 'o in oldObject.metadata.?ownerReferences.orValue([])'; do
  grep -Fq "$expected" <<<"$rendered" || { echo "missing admission policy setting: $expected" >&2; exit 1; }
done
grep -Fq "system:serviceaccount:monitoring:custom-sa'" <<<"$(helm template health "$chart_dir" --namespace monitoring "${vap[@]}" --set serviceAccount.name=custom-sa)" \
  || { echo 'admission policy must match the configured ServiceAccount' >&2; exit 1; }
for args in "" "${vap[*]} --set rbac.admissionPolicy.enabled=false" "${vap[*]} --set precedence.enabled=false"; do
  # shellcheck disable=SC2086
  if helm template health "$chart_dir" --namespace monitoring $args | grep -Fq 'ValidatingAdmissionPolicy'; then
    echo "admission policy rendered unexpectedly with: ${args:-API unavailable}" >&2
    exit 1
  fi
done
echo 'scrape patch admission policy renders passed'
