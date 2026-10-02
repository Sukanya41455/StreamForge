# StreamForge Completion Design

**Purpose:** Finish the existing local StreamForge demonstration by correcting the persisted summary view, making the kind deployment runnable, and recording fresh verification evidence.

## Verified findings

- `minute_metrics` has separate latest windows for order and payment queries. Selecting the single newest window returns a payment row but omits the previous minute's order and revenue rows.
- The Kubernetes `kafka` Service injects `KAFKA_PORT` into its own Pod. The Confluent image treats it as deprecated broker configuration and exits before becoming ready. After that is removed, a broker listener must bind to the Pod interface rather than the Service virtual IP.
- The kind cluster has no `metrics.k8s.io` API, so the API HPA cannot calculate CPU utilization.

## Scope

1. The summary repository will fetch the newest persisted row independently for `orders`, `revenue`, and `payment_success_rate`. The API response shape remains unchanged.
2. The Kafka Deployment will set `enableServiceLinks: false`, bind KRaft listeners to all Pod interfaces, use a loopback controller voter, and publish the single broker's Service endpoint during readiness so its advertised client address remains reachable.
3. A pinned metrics-server v0.9.0 manifest, compatible with Kubernetes 1.37, will be vendored under a separate `kubernetes/metrics-server` Kustomization. Its Deployment will use `--kubelet-insecure-tls`, which is required for the local kind kubelet certificate setup. The application Kustomization remains namespaced to `streamforge`; metrics-server stays in `kube-system`.
4. Documentation will state the exact image-load, metrics-server, and application apply sequence, plus the commands that prove readiness and HPA metrics.
5. Verification will include the full Python test suite, a live Compose summary read, Kubernetes rollout and `kubectl top` checks, a streaming restart observation, and a 60-second bounded load observation. Only actual measurements will be written to benchmark and failure records.

## Non-goals

- No multi-broker Kafka, persistent Kubernetes volumes, external registry, CI pipeline, or production delivery guarantee.
- No claim of exactly-once processing or a throughput maximum without a recorded experiment.

## Acceptance criteria

- A repository test fails under skewed metric windows and passes after the fix.
- `/analytics/summary` returns non-zero persisted order and revenue values in the live Compose run.
- `kubectl get pods -n streamforge` shows the Kafka and producer Pods ready; `kubectl top pods -n streamforge` succeeds; and the HPA reports a valid CPU metric.
- Fresh test and experiment results are recorded or an explicit blocking command/error is recorded instead.
