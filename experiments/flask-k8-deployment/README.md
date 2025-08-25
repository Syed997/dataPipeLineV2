# Run INFO:

# 1. Update the otel collector endpoint in the app1.py

```
metric_exporter = OTLPMetricExporter(endpoint="http://host.minikube.internal:4317", insecure=True)
trace_exporter = OTLPSpanExporter(endpoint="http://host.minikube.internal:4317", insecure=True)
otlp_log_exporter = OTLPLogExporter(endpoint="http://host.minikube.internal:4317", insecure=True)
```

# 2. Build docker image:

```
docker build -t flask-otel-app:1.0 .
```

# 3. Deployment command in k8:

```
kubectl apply -f deployment.yaml
```

---

# Check

```
oteloutput/demo-flask-logs.json
oteloutput/demo-flask-traces.json
oteloutput/demo-flask-metrics.json
```
