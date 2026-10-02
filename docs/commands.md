# StreamForge command reference

This guide keeps operational commands out of the portfolio README. Run them
from the repository root in PowerShell.

## Local Docker Compose stack

Start the complete local topology:

```powershell
docker compose up --build
```

Run it in the background:

```powershell
docker compose up --build -d
```

Stop the stack while preserving named volumes:

```powershell
docker compose down
```

Inspect the local endpoints:

```text
API health:    http://localhost:8000/healthz
API metrics:   http://localhost:8000/metrics
Prometheus:    http://localhost:9090
Grafana:       http://localhost:3000  (admin / admin)
```

## Tests and event inspection

Run the complete Python test suite:

```powershell
.\.venv\Scripts\python.exe -m pytest -v
```

Generate a small deterministic event sample:

```powershell
python -m producer.main --count 4 --seed 7
```

List Kafka topics from the running Compose stack:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Read persisted analytics:

```powershell
Invoke-RestMethod http://localhost:8000/analytics/summary
Invoke-RestMethod http://localhost:8000/analytics/top-products
```

## Local Kubernetes with kind

Build the application image, create a local cluster, load the image into it,
then install the metric provider before the application manifests:

```powershell
docker build -t streamforge-api:latest .
kind create cluster --name streamforge
kind load docker-image streamforge-api:latest --name streamforge
kubectl apply -k kubernetes/metrics-server
kubectl rollout status deployment/metrics-server -n kube-system
kubectl apply -k kubernetes
```

Verify workload health, resource metrics, and API access:

```powershell
kubectl get pods -n streamforge
kubectl top pods -n streamforge
kubectl get hpa -n streamforge
kubectl port-forward -n streamforge service/api 8000:8000
```

The local Metrics Server manifest uses `--kubelet-insecure-tls` for `kind`.
Do not carry that flag into a production cluster with trusted kubelet
certificates.

## Controlled experiments

Restart the streaming job and inspect the persisted summary afterward:

```powershell
docker compose restart streaming
Invoke-RestMethod http://localhost:8000/analytics/summary
```

Run a temporary additional producer for a bounded load observation, then
remove it:

```powershell
docker compose run -d --name streamforge-load-producer --no-deps -e EVENTS_PER_SECOND=50 producer
docker stop streamforge-load-producer
docker rm streamforge-load-producer
```

To explore duplicate, late, or payment-failure behavior, set `SCENARIO` for
the producer in `docker-compose.yml`, restart that service, and record the
observed aggregate and lag behavior in `docs/failure-analysis.md`.

## Useful logs

```powershell
docker compose logs -f streaming
docker compose logs -f producer
kubectl logs -n streamforge deployment/streaming -f
kubectl logs -n streamforge deployment/producer -f
```

For the interpretation of these experiments and their existing measurements,
see [failure-analysis.md](failure-analysis.md) and [benchmarks.md](benchmarks.md).
