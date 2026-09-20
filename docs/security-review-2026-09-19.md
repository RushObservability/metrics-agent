# Metrics-agent security review

Reviewed against main commit `4921d3d`. This review covers the Rust service, embedded UI, Helm defaults, and locked dependencies. It is a code review with regression tests, not a penetration test of a running cluster or a container-image scan.

## Findings fixed

### 1. Scrape destination checks could be bypassed

Medium severity. A user able to create a scrape CRD in an allowed namespace could use an IPv6 literal to reach a destination that should be blocked. URL host strings retained IPv6 brackets, so parsing them as IP addresses failed. Reqwest connects to IP literals without calling the custom DNS resolver. IPv4-mapped IPv6 addresses also escaped the IP denylist.

Environment HTTP proxies could bypass the custom resolver by resolving the target on the proxy. Scraping now disables environment proxies, checks bracketed IP literals, and applies IPv4 restrictions to mapped addresses. Explicit administrator destination exceptions still work.

The denylist also covers the documented [AWS IPv6 metadata endpoint](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html) and [Alibaba metadata endpoint](https://www.alibabacloud.com/help/en/ecs/user-guide/view-instance-metadata/). Ordinary private cluster addresses remain allowed.

Tests cover blocked IPv6 literals, mapped DNS answers, metadata endpoints, private cluster addresses, explicit overrides, and a child process configured with HTTP proxy environment variables. The IPv6 regression test failed on the original code at `[::1]`.

### 2. Remote write followed redirects

Medium severity. The default HTTP client followed redirects. A remote-write endpoint could redirect a POST with a 307 or 308 and send its telemetry body and custom tenant header to another origin. Standard Authorization stripping does not protect the body or custom headers.

The production remote-write client now refuses redirects. Configure the final destination URL. Tests exercise 301, 302, 303, 307, and 308 responses with a separate destination server and verify it receives no request.

### 3. Small scrape responses could expand into large allocations

Medium severity. The response-size limit did not limit decoded samples. Target labels were copied into every sample; HELP text was copied when parsing finished. A 4 KiB HELP value repeated across 50,000 samples alone needs about 195 MiB, even when the response is much smaller.

The parser now accounts for retained samples, labels, and metadata. It checks all HELP copies before allocating them, including metadata received after samples. Exceeding the budget rejects the target's entire scrape rather than publishing partial results.

The default per-target budget is 8 MiB. Change it with:

```yaml
scrape:
  maxRetainedBytes: 8388608
```

The equivalent environment variable is `METRICS_AGENT_SCRAPE_MAX_RETAINED_BYTES`; the CLI flag is `--scrape-max-retained-bytes`.

This is decoded-data accounting, not a hard process RSS limit. Allow headroom for allocator overhead, input buffers, discovery, queued batches, and remote-write encoding. Scrape concurrency multiplies the per-target budget. Keep Kubernetes memory limits enabled.

### 4. A separate UI bind did not isolate diagnostic routes

Medium severity. When the UI used its own listener, the primary listener still registered the same UI and diagnostic routes. Setting a loopback UI address did not prevent exposure through the primary listener.

The primary listener now serves only health and Prometheus metrics unless it intentionally shares the configured UI address. A separate UI listener owns the UI and detailed diagnostic APIs. Router tests cover separate and shared configurations.

### 5. Configuration formatting exposed credentials

Low severity. Clap help could display the remote-write token and URL supplied through environment variables. Derived Debug output also included their raw values.

Help now hides both environment values. Debug output reports whether remote write and its token are configured, without printing them. Tests use dummy secrets in the actual binary's help output and in Config formatting.

### 6. A vulnerable TLS dependency and a yanked patch were locked

RustSec reported [RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285/) against rustls 0.23.42. The lockfile now uses 0.23.45, with its compatible webpki patch. The handshake transcript was still authenticated in the affected version; the advisory does not describe an attacker completing an unauthenticated handshake.

The yanked chacha20 0.10.1 release was updated to 0.10.2. A yanked release is not itself evidence of an exploitable vulnerability. Dependency changes are limited to these patches and the existing tower dependency being declared for router tests.

## Deployment boundaries that still matter

- The optional UI has no application login. Keep it disabled, bind it privately, or protect it with authenticated access and a restrictive NetworkPolicy.
- Destination exceptions are administrator-controlled bypasses. Do not populate them from untrusted scrape objects.
- Blackbox probes delegate their target requests to another service. That prober needs its own destination and egress restrictions; the agent's resolver protects connections made by the agent, not requests made by the prober.
- Private workload addresses are intentionally reachable. Restrict who can create scrape CRDs and which source namespaces the agent accepts.
- Use HTTPS for remote write outside a trusted local network. The agent retains normal certificate verification.
- Per-target parser budgets do not cap the number of Kubernetes objects or discovered targets. Cluster RBAC, quotas, memory limits, and scrape concurrency remain part of capacity and availability protection.

No root README, release workflow, running deployment, or cluster configuration was changed.
