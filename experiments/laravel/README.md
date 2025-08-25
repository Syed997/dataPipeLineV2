# Run INFO:

# 1. Edit the .env file according to the host machine ip

```
OTEL_SERVICE_NAME=laravel
OTEL_EXPORTER_OTLP_ENDPOINT=http://172.17.0.1:4318/v1/traces
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://172.17.0.1:4318/v1/traces #docker
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://172.17.0.1:4318/v1/logs
OTEL_PHP_AUTOLOAD_ENABLED=true
OTEL_TRACES_EXPORTER=otlp
OTEL_EXPORTER_OTLP_PROTOCOL=http/json
OTEL_LOG_LEVEL=info
# # OTEL_LOG_LEVEL=none #to disable logs
OTEL_PROPAGATORS=baggage,tracecontext
OTEL_ENABLED=true
```

# 2. Run the project:

```
cd laravel-otel-instrument

docker-compose up -d --build
```

# 3. Hit laravel app's API

```
./call_api.sh
```

---

# Check

```
oteloutput/demo-laravel-logs.json
oteloutput/demo-laravel-traces.json
oteloutput/demo-laravel-metrics.json
```
