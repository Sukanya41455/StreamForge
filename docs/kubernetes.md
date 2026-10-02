# Local Kubernetes

`kind` runs StreamForge without a cloud account.

```powershell
kind create cluster --name streamforge
kind load docker-image streamforge-api:latest --name streamforge
kubectl apply -k kubernetes/metrics-server
kubectl rollout status deployment/metrics-server -n kube-system
kubectl apply -k kubernetes
kubectl get pods -n streamforge
kubectl top pods -n streamforge
kubectl port-forward -n streamforge service/api 8000:8000
```

Build the local image before loading it: `docker build -t streamforge-api:latest .`.
The vendored metrics-server v0.9.0 manifest supplies the Metrics API used by
the HPA. Its local kind configuration uses `--kubelet-insecure-tls`; do not
reuse that setting for a production cluster with trusted kubelet certificates.

The API starts with two replicas and its HPA permits two through five.

Verified on 2026-10-01: all StreamForge Pods were Ready, `kubectl top pods`
returned resource usage, and the API HPA reported CPU utilization. The loaded
application image was `sha256:46c37ac5c20cf00db8b91b327fb4b0f849811522e2770044e7f05e302d7d50fc`.

The local Kafka/Postgres deployments intentionally use ephemeral storage. They
are for studying deployment mechanics, recovery, probes, and scaling—not for
claiming durable production storage.
