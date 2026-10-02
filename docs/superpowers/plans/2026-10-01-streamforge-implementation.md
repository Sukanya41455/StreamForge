# StreamForge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fully local real-time marketplace analytics platform that is demonstrable, measurable, and structured for interview study.

**Architecture:** Versioned events flow from a deterministic Python producer through Kafka into PySpark Structured Streaming. Spark validates and deduplicates event-time records, upserts aggregates into PostgreSQL, and exposes processing metrics; FastAPI serves those aggregates. Compose runs the development stack, while kind manifests reproduce it locally with probes and HPA.

**Tech Stack:** Python 3.12+, pytest, Kafka (KRaft), PySpark Structured Streaming, PostgreSQL, FastAPI, Prometheus client, Grafana, Docker Compose, Kubernetes, kind.

**Spec:** `docs/superpowers/specs/2026-10-01-streamforge-design.md`

## Global Constraints

- Entirely local and free; no cloud or managed services.
- Start with one Kafka broker, one Spark process, and PostgreSQL; do not claim exactly-once delivery.
- All events are versioned JSON and include a stable `event_id` and UTC `event_time`.
- Record only measured benchmark and recovery figures.
- Use application health and `/metrics` endpoints for operational evidence.
- Never run `git add`, commit, push, create a branch, or otherwise alter Git history; leave all changes in the working tree.

## Review Focus

- Malformed Kafka JSON must reach a dead-letter record and not terminate the streaming query (Task 3).
- A duplicate `event_id` within the watermark horizon must affect an aggregate once (Task 3).
- An event later than the configured watermark must be observable as late/dropped behavior (Task 3).
- Replaying an aggregate batch must preserve a single Postgres row per metric/window key (Task 3).
- Empty database results must produce valid, zero/empty API responses rather than server errors (Task 4).

---

### Task 1: Repository foundation and event producer

**Files:**
- Create: `pyproject.toml`, `producer/models.py`, `producer/generator.py`, `producer/main.py`, `tests/test_models.py`, `tests/test_generator.py`
- Modify: `README.md`

**Interfaces:**
- Produces: `OrderEvent`, `PaymentEvent`, `Scenario`, and `generate_events(config: GeneratorConfig) -> Iterator[Event]`.
- Consumed by: Kafka publisher in Task 2 and load scripts in Task 6.

- [ ] **Step 1: Write failing event-contract tests**

Test valid serialization, rejected negative amounts, UTC timestamps, deterministic seeded generation, and duplicate/late scenario behavior.

- [ ] **Step 2: Run the focused tests**

Run: `pytest tests/test_models.py tests/test_generator.py -v`  
Expected: FAIL because the producer modules do not exist.

- [ ] **Step 3: Implement versioned Pydantic event models and deterministic generator**

`generate_events` accepts rate, seed, and scenario settings; it yields only `OrderEvent` and `PaymentEvent` JSON-ready models.

- [ ] **Step 4: Run focused tests and add the local setup README section**

Run: `pytest tests/test_models.py tests/test_generator.py -v`  
Expected: PASS.

- [ ] **Step 5: Review the working-tree change set**

Run: `git diff --check`  
Expected: no whitespace errors; do not stage or commit changes.

### Task 2: Kafka and PostgreSQL local infrastructure

**Files:**
- Create: `docker-compose.yml`, `docker/kafka/create-topics.sh`, `database/init.sql`, `producer/kafka_publisher.py`, `tests/test_kafka_publisher.py`
- Modify: `producer/main.py`, `README.md`

**Interfaces:**
- Consumes: `generate_events(config)` from Task 1.
- Produces: `publish_events(events: Iterable[Event], settings: KafkaSettings) -> PublishStats`; Kafka topics and database tables from the spec.
- Consumed by: Spark job in Task 3.

- [ ] **Step 1: Write a failing publisher test with a fake producer**

Assert that each event goes to its versioned topic, uses `order_id` as a key, and flushes on completion.

- [ ] **Step 2: Run the publisher test**

Run: `pytest tests/test_kafka_publisher.py -v`  
Expected: FAIL because `publish_events` does not exist.

- [ ] **Step 3: Implement publisher and Compose services**

Create KRaft Kafka, Postgres initialization, topic creation, and a publisher that retries normal broker-startup races only.

- [ ] **Step 4: Run unit test and Compose smoke check**

Run: `pytest tests/test_kafka_publisher.py -v` then `docker compose up -d kafka postgres && docker compose ps`  
Expected: test PASS and both services healthy/running.

- [ ] **Step 5: Review the working-tree change set**

Run: `git diff --check`  
Expected: no whitespace errors; do not stage or commit changes.

### Task 3: Structured Streaming and idempotent aggregates

**Files:**
- Create: `streaming/schemas.py`, `streaming/transforms.py`, `streaming/postgres_sink.py`, `streaming/main.py`, `tests/test_transforms.py`, `tests/test_postgres_sink.py`
- Modify: `docker-compose.yml`, `database/init.sql`

**Interfaces:**
- Consumes: Kafka event contract/topics from Tasks 1–2.
- Produces: `build_order_aggregates(stream, watermark: str)`, `build_payment_aggregates(stream, watermark: str)`, and `upsert_batch(frame, table: str, connection_url: str) -> None`.
- Consumed by: API in Task 4.

- [ ] **Step 1: Write failing transformation and upsert tests**

Use local Spark fixtures to assert schema rejection, watermark-based duplicate suppression, minute windows, payment success metrics, and replay-safe composite-key upserts.

- [ ] **Step 2: Run focused streaming tests**

Run: `pytest tests/test_transforms.py tests/test_postgres_sink.py -v`  
Expected: FAIL because transformation/sink functions do not exist.

- [ ] **Step 3: Implement parsing, dead-letter routing, watermarks, aggregations, and Postgres foreach-batch sink**

Use a 10-minute event-time watermark, `event_id` deduplication, one-minute metrics, and five-minute product windows. Each `foreachBatch` write uses `INSERT ... ON CONFLICT DO UPDATE`.

- [ ] **Step 4: Run focused tests and the Compose pipeline smoke check**

Run: `pytest tests/test_transforms.py tests/test_postgres_sink.py -v` then produce a bounded event set and query Postgres.  
Expected: tests PASS; aggregate rows appear once per window key.

- [ ] **Step 5: Review the working-tree change set**

Run: `git diff --check`  
Expected: no whitespace errors; do not stage or commit changes.

### Task 4: Query API and application metrics

**Files:**
- Create: `api/main.py`, `api/repository.py`, `api/models.py`, `tests/test_api.py`, `tests/test_repository.py`
- Modify: `pyproject.toml`, `docker-compose.yml`

**Interfaces:**
- Consumes: `minute_metrics` and `product_window_metrics` from Task 3.
- Produces: `GET /healthz`, `GET /metrics`, `GET /analytics/summary`, `GET /analytics/top-products`.
- Consumed by: Prometheus/Grafana in Task 5 and Kubernetes probes in Task 7.

- [ ] **Step 1: Write failing API/repository tests**

Assert healthy status, an empty-but-valid summary, populated summaries, validated top-product limits, and Prometheus request metric output.

- [ ] **Step 2: Run focused API tests**

Run: `pytest tests/test_api.py tests/test_repository.py -v`  
Expected: FAIL because API modules do not exist.

- [ ] **Step 3: Implement read-only repository and FastAPI routes**

Use parameterized SQL and return typed response models; instrument request count/latency and database readiness.

- [ ] **Step 4: Run API tests and HTTP smoke check**

Run: `pytest tests/test_api.py tests/test_repository.py -v` then `curl http://localhost:8000/healthz`  
Expected: tests PASS and HTTP 200.

- [ ] **Step 5: Review the working-tree change set**

Run: `git diff --check`  
Expected: no whitespace errors; do not stage or commit changes.

### Task 5: Observability and one-command local operation

**Files:**
- Create: `monitoring/prometheus/prometheus.yml`, `monitoring/grafana/dashboards/streamforge.json`, `monitoring/grafana/provisioning/datasources/prometheus.yml`, `monitoring/grafana/provisioning/dashboards/dashboards.yml`, `docs/operations.md`
- Modify: `docker-compose.yml`, `README.md`

**Interfaces:**
- Consumes: application `/metrics`, Kafka exporter metrics, and Compose service names.
- Produces: provisioned StreamForge dashboard and two alert rules: high consumer lag and elevated payment failures.

- [ ] **Step 1: Write a configuration assertion test/script**

Assert Prometheus contains every scrape target and dashboard JSON contains orders/revenue, consumer lag, batch duration, API latency, and error-rate panels.

- [ ] **Step 2: Run the configuration check**

Run: `pytest tests/test_monitoring_config.py -v`  
Expected: FAIL before the monitoring files exist.

- [ ] **Step 3: Implement Prometheus, Grafana provisioning, dashboard, alerts, and operator instructions**

Give every metric panel a unit and a clear title; document start/stop/reset commands without destructive defaults.

- [ ] **Step 4: Run configuration check and inspect service health**

Run: `pytest tests/test_monitoring_config.py -v` then `docker compose up -d && docker compose ps`  
Expected: test PASS and monitoring services running.

- [ ] **Step 5: Review the working-tree change set**

Run: `git diff --check`  
Expected: no whitespace errors; do not stage or commit changes.

### Task 6: Benchmarks and failure experiments

**Files:**
- Create: `load_tests/run_load.py`, `failure_tests/restart_streaming.ps1`, `failure_tests/duplicate_events.ps1`, `failure_tests/late_events.ps1`, `docs/benchmarks.md`, `docs/failure-analysis.md`, `tests/test_load_config.py`

**Interfaces:**
- Consumes: producer `GeneratorConfig`, Compose service names, API summary endpoint, Prometheus metrics.
- Produces: machine-readable run metadata and human-readable result templates; no prefilled performance claims.

- [ ] **Step 1: Write failing load-config tests**

Assert invalid rates/durations fail clearly and result records contain input rate, duration, partition count, and observed response data.

- [ ] **Step 2: Run focused test**

Run: `pytest tests/test_load_config.py -v`  
Expected: FAIL because load configuration code does not exist.

- [ ] **Step 3: Implement bounded load runner and failure scripts**

Default to a safe 60-second run. Restart only the named streaming container; scripts capture checkpoints, API output, and relevant service state.

- [ ] **Step 4: Run test and one safe experiment**

Run: `pytest tests/test_load_config.py -v` then `python load_tests/run_load.py --rate 50 --duration-seconds 60`  
Expected: test PASS and a timestamped result record.

- [ ] **Step 5: Review the working-tree change set**

Run: `git diff --check`  
Expected: no whitespace errors; do not stage or commit changes.

### Task 7: Local Kubernetes deployment

**Files:**
- Create: `kubernetes/namespace.yaml`, `kubernetes/configmap.yaml`, `kubernetes/postgres.yaml`, `kubernetes/kafka.yaml`, `kubernetes/streaming.yaml`, `kubernetes/api.yaml`, `kubernetes/producer.yaml`, `kubernetes/monitoring.yaml`, `kubernetes/api-hpa.yaml`, `kubernetes/kustomization.yaml`, `docs/kubernetes.md`

**Interfaces:**
- Consumes: container images, environment variables, `/healthz`, and `/metrics` from Tasks 1–5.
- Produces: `streamforge` namespace with resource requests/limits, readiness/liveness probes, services, and 2–5 replica API HPA.

- [ ] **Step 1: Write manifest validation test/script**

Assert all workloads use the `streamforge` namespace, API probes target `/healthz`, resource requests/limits exist, and HPA min/max replicas are 2/5.

- [ ] **Step 2: Run validation before manifests exist**

Run: `pytest tests/test_kubernetes_manifests.py -v`  
Expected: FAIL because Kubernetes manifests do not exist.

- [ ] **Step 3: Implement kind instructions and declarative manifests**

Use the same event/database configuration as Compose. Document image loading, apply order, port-forwarding, smoke checks, and HPA observation.

- [ ] **Step 4: Run manifest validation and kind smoke checks**

Run: `pytest tests/test_kubernetes_manifests.py -v` then `kubectl apply --dry-run=client -k kubernetes`  
Expected: test PASS and client-side manifests valid.

- [ ] **Step 5: Review the working-tree change set**

Run: `git diff --check`  
Expected: no whitespace errors; do not stage or commit changes.

### Task 8: Final project evidence and interview guide

**Files:**
- Create: `docs/interview-guide.md`, `docs/architecture.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: verified commands/results and dashboard screenshots from Tasks 1–7.
- Produces: an honest project narrative, setup path, troubleshooting guide, experiment tables, and question/answer prompts.

- [ ] **Step 1: Write documentation completeness checks**

Assert README has architecture, quick start, metrics, benchmarks, failure experiments, Kubernetes, and a no-fabricated-results rule.

- [ ] **Step 2: Run the documentation check**

Run: `pytest tests/test_readme.py -v`  
Expected: FAIL before final documentation exists.

- [ ] **Step 3: Write final evidence-based documentation**

Explain offsets, partitioning, at-least-once effects, idempotent upserts, watermarks, checkpoint recovery, consumer lag, and HPA trade-offs using this implementation’s evidence.

- [ ] **Step 4: Run full verification**

Run: `pytest -v` and the Compose smoke path documented in README.  
Expected: all tests PASS; only measured experiment fields are populated.

- [ ] **Step 5: Review the working-tree change set**

Run: `git diff --check`  
Expected: no whitespace errors; do not stage or commit changes.
