# StreamForge

StreamForge is a local real-time marketplace analytics platform built to study
Kafka, PySpark Structured Streaming, PostgreSQL, FastAPI, Prometheus, Grafana,
and Kubernetes through one connected system.

```text
Python producer -> Kafka -> Spark streaming -> PostgreSQL -> FastAPI
                     |                         |
                     +---- exporter -------- Prometheus -> Grafana
```

## Quick start

Start Docker Desktop, then run:

```powershell
docker compose up --build
```

Useful local endpoints:

- API: `http://localhost:8000/healthz`
- API metrics: `http://localhost:8000/metrics`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (default `admin` / `admin`)

Run the test suite with:

```powershell
.\.venv\Scripts\python.exe -m pytest -v
```

## What each component does

- **Producer:** creates deterministic versioned order and payment events.
  `duplicates`, `late`, and `payment_failures` scenarios make correctness
  behavior observable.
- **Kafka:** durably separates producers from consumers. Topic partitions and
  consumer offsets are visible infrastructure concepts, not hidden plumbing.
- **Spark:** validates JSON, writes malformed records to the dead-letter topic,
  uses event-time watermarks, deduplicates by `event_id`, and calculates minute
  and five-minute window aggregates.
- **PostgreSQL:** stores idempotently upserted aggregate rows that can be
  safely replayed after a Spark checkpoint recovery.
- **FastAPI:** serves persisted results and emits HTTP request/latency metrics.
- **Prometheus/Grafana:** turn API latency and Kafka consumer lag into an
  operational dashboard and alert conditions.
- **Kubernetes:** runs the same services with probes, resource limits, and an
  API HPA. See [kubernetes.md](docs/kubernetes.md).

## Study path

1. Generate and inspect events: `python -m producer.main --count 4 --seed 7`.
2. Start Kafka/Postgres and inspect topics with `docker compose exec kafka
   kafka-topics --bootstrap-server kafka:29092 --list`.
3. Watch Spark checkpoint and watermark behavior while switching `SCENARIO` in
   Compose.
4. Query `/analytics/summary` and `/analytics/top-products`.
5. Correlate Kafka lag and API latency in Grafana.
6. Restart `streaming`, increase `EVENTS_PER_SECOND`, and record measured
   recovery behavior in `docs/benchmarks.md`.

Never add performance or data-loss claims to this README unless you can point
to the command and result that measured them.
