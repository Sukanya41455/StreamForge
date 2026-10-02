# StreamForge

**A production-minded real-time analytics platform for marketplace events.**

StreamForge turns a stream of versioned order and payment events into
queryable marketplace metrics. It is a hands-on demonstration of the concerns
that make streaming systems interesting in production: asynchronous delivery,
late and duplicate events, stateful computation, idempotent persistence,
observability, and deployment health.

It is intentionally a local learning platform, not a claim of production
scale. The design makes its trade-offs explicit and records the experiments
that have actually been run.

## Architecture

```text
Python event producer
        |
        v
Apache Kafka  <---- Kafka exporter
        |                 |
        v                 v
PySpark Structured      Prometheus  --->  Grafana
Streaming                     ^
        |                     |
        v                     |
PostgreSQL  <--- FastAPI ------+
```

The producer creates deterministic, schema-versioned order and payment events.
Kafka decouples event creation from processing. PySpark Structured Streaming
validates and transforms events, then persists aggregates to PostgreSQL. A
FastAPI service exposes those persisted analytics while Prometheus and Grafana
make service health, request latency, and consumer lag visible. The same
application topology can also run on a local Kubernetes cluster.

## What this project demonstrates

### Event streaming and distributed-systems reasoning

- **Kafka as a durable event log:** producers and consumers are independently
  deployable, and consumer offsets make replay and recovery concrete rather
  than theoretical.
- **At-least-once delivery with idempotent effects:** retries can produce
  duplicates, so aggregate writes use a composite-key upsert to make replay
  safe at the PostgreSQL boundary. This is deliberately not labeled
  end-to-end exactly-once processing.
- **Event time over processing time:** minute and five-minute aggregates use
  event timestamps. Watermarks bound retained state and make the late-data
  policy explicit.
- **Failure-aware input handling:** malformed events are routed to a
  dead-letter topic, while duplicate and late-event producer scenarios provide
  controlled ways to observe correctness behavior.

### PySpark Structured Streaming

- Stateful window aggregation and event-time watermarks.
- Deduplication keyed by `event_id`.
- Checkpointed query progress and state metadata, allowing a restarted stream
  to resume without blindly starting from the beginning.
- A `foreachBatch` PostgreSQL sink that persists aggregates for serving rather
  than coupling the API directly to a running Spark job.

### Production observability

- FastAPI publishes Prometheus request-count and latency metrics.
- Kafka exporter surfaces consumer lag, a useful backpressure and throughput
  signal.
- Grafana is provisioned with dashboards, while Prometheus evaluates alert
  rules, so the system can be observed as a whole rather than component by
  component.
- Recorded failure and load observations distinguish measured behavior from
  unverified performance claims.

### Kubernetes operations

- Deployments, Services, readiness/liveness probes, resource requests and
  limits, and a Horizontal Pod Autoscaler for the API.
- A single-node KRaft Kafka configuration designed to work on local `kind`,
  including explicit listener and readiness behavior.
- Metrics Server is deployed separately so the HPA receives CPU metrics.
- The Kubernetes manifests use ephemeral storage by design; they demonstrate
  operational mechanics, not durable production storage.

## Engineering decisions worth discussing

| Decision | Why it matters |
| --- | --- |
| Persist stream aggregates in PostgreSQL | The API serves stable, replay-safe results even though Spark is asynchronous. |
| Read the newest value per metric | Independent event streams can advance at different times; a shared latest window can silently zero valid metrics. |
| Watermark and deduplicate in Spark | Stateful streaming needs a bounded late-data policy and protection against retry-driven duplicates. |
| Treat Kafka lag as an operational signal | Lag identifies consumer throughput or backpressure problems; it does not by itself prove data loss. |
| Scale the API, not the entire pipeline | CPU HPA can add API replicas, but Kafka partitioning, Spark state, and database capacity remain separate bottlenecks. |

## Validation evidence

The current recorded checks include:

- The Python suite passes with **18 tests**.
- A live Compose run returned non-zero persisted summary metrics after the
  per-metric summary-query correction.
- A local `kind` run reached Ready Pods; Metrics Server supplied resource
  usage and the API HPA reported CPU utilization.
- A streaming restart completed in **20.53 seconds** in the recorded local
  observation, followed by a non-zero analytics summary.
- A bounded local load observation recorded **25.1 order records/second**.
  That is an observed result under the documented conditions, not a capacity
  ceiling.

See the [benchmark record](docs/benchmarks.md) and
[failure experiments](docs/failure-analysis.md) for conditions and limits of
those observations.

## Project guide

- [Command reference](docs/commands.md) — local stack, tests, Kubernetes,
  observability, and experiments.
- [Kubernetes notes](docs/kubernetes.md) — local-cluster design and caveats.
- [Failure experiments](docs/failure-analysis.md) — recovery and load
  exercises with recorded results.
- [Benchmark record](docs/benchmarks.md) — measured performance observations.
- [Interview guide](docs/interview-guide.md) — concise explanations of the
  key distributed-systems decisions.

## Scope and honest limitations

StreamForge uses a single Kafka broker and ephemeral local Kubernetes storage.
It does not provide multi-zone durability, a managed schema registry, CI/CD,
an external container registry, or a full production SLA. Those omissions are
intentional: the repository focuses on making the core streaming and
operations concepts inspectable end to end.

For setup and operational commands, use the [command reference](docs/commands.md).
