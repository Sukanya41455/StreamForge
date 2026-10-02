# StreamForge Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Correct the analytics summary, make the kind deployment operational, and leave reproducible verification evidence.

**Architecture:** Read each summary metric from its own newest stored window. Keep app manifests namespaced and deploy cluster-scoped metrics-server from a separate, pinned Kustomization. Use the existing Compose and kind environments as acceptance-test targets.

**Tech Stack:** Python 3.12, pytest, PostgreSQL, FastAPI, Kubernetes/Kustomize, kind, metrics-server v0.9.0.

**Spec:** `docs/superpowers/specs/2026-10-01-streamforge-completion-design.md`

## Global Constraints

- Preserve the public API response shape and existing single-broker Kafka topology.
- Vendor metrics-server v0.9.0 separately from the namespaced application Kustomization.
- Record measured results only; do not claim exactly-once delivery or unmeasured performance.

## Review Focus

- Skewed rows where the latest metric is payment-only must return the latest order and revenue too.
- The broker must not receive a service-link `KAFKA_PORT` variable.
- Metrics-server must remain in `kube-system`, not `streamforge`.
- Kind metrics collection must use its local kubelet TLS workaround.
- Documentation must make image loading happen before applying the app workload.

### Task 1: Correct persisted summary selection

**Files:**
- Modify: `api/repository.py`
- Modify: `tests/test_repository.py`

**Interfaces:**
- Produces: `PostgresRepository.summary() -> dict[str, float]` with the newest persisted value for each required metric.

- [ ] **Step 1: Write the failing repository test**

Make the fake cursor return `orders` and `revenue` from one minute plus `payment_success_rate` from the next. Assert all three are returned.

- [ ] **Step 2: Run the targeted test to verify it fails**

Run: `./.venv/Scripts/python.exe -m pytest tests/test_repository.py -v`
Expected: FAIL because the current query selects one shared latest window.

- [ ] **Step 3: Select each metric's newest row in `api/repository.py`**

Use a PostgreSQL window function partitioned by `metric_name`, filtering only the three public summary metrics.

- [ ] **Step 4: Run the targeted test to verify it passes**

Run: `./.venv/Scripts/python.exe -m pytest tests/test_repository.py -v`
Expected: PASS.

### Task 2: Repair the kind platform manifests

**Files:**
- Modify: `kubernetes/kafka.yaml`
- Create: `kubernetes/metrics-server/components.yaml`
- Create: `kubernetes/metrics-server/kustomization.yaml`
- Modify: `docs/kubernetes.md`

**Interfaces:**
- Consumes: the existing `kafka` Service and kind cluster.
- Produces: a Kafka Pod without service-link environment variables and a `metrics.k8s.io/v1beta1` API provided by metrics-server v0.9.0.

- [ ] **Step 1: Make the minimal manifest and documentation changes**

Set `enableServiceLinks: false` under the Kafka Pod spec. Vendor the official v0.9.0 components manifest and patch its Deployment arguments with `--kubelet-insecure-tls`; add an isolated Kustomization and documented apply order.

- [ ] **Step 2: Render and validate the manifests**

Run: `kubectl kustomize kubernetes/metrics-server` and `kubectl kustomize kubernetes`
Expected: both render valid resources, with metrics-server in `kube-system` and Kafka service links disabled.

### Task 3: Rebuild, apply, and record live evidence

**Files:**
- Modify: `docs/benchmarks.md`
- Modify: `docs/failure-analysis.md`
- Modify: `docs/superpowers/plans/2026-10-01-streamforge-progress.md`

**Interfaces:**
- Consumes: the corrected application and manifests.
- Produces: fresh commands/results for Compose summary, cluster readiness/HPA metrics, restart recovery, and a bounded load run.

- [ ] **Step 1: Run the complete test suite**

Run: `./.venv/Scripts/python.exe -m pytest -v`
Expected: PASS.

- [ ] **Step 2: Rebuild Compose and validate summary output**

Run: `docker compose up --build -d` then query `/analytics/summary`.
Expected: a persisted non-zero order and revenue value after streaming processes a complete minute.

- [ ] **Step 3: Rebuild/load image and apply Kubernetes resources**

Run: `docker build -t streamforge-api:latest .`, `kind load docker-image streamforge-api:latest --name streamforge`, `kubectl apply -k kubernetes/metrics-server`, then `kubectl apply -k kubernetes`.
Expected: Kafka and producer become ready; `kubectl top pods -n streamforge` succeeds; HPA reports CPU metrics.

- [ ] **Step 4: Run restart and bounded-load experiments**

Restart streaming, then run a 60-second `EVENTS_PER_SECOND=50` Compose load. Record exact outcomes and any blocker in the documentation.
