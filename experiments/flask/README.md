# Run INFO:

Create , Install & Activate virtual env `v3` using requirements.txt


# 1. Install prerequisites
sudo apt update && sudo apt install -y python3 python3-pip
pip install flask opentelemetry-distro opentelemetry-exporter-otlp
pip install opentelemetry-instrumentation-flask requests

# 2. Auto-Instrument Flask Apps

Instead of modifying code for OTel, we’ll run them with:

`opentelemetry-bootstrap -a install`  # installs required instrumentation

# Run in Separate Terminals:

# App1
OTEL_SERVICE_NAME=app1 \
OTEL_EXPORTER_OTLP_ENDPOINT=http://0.0.0.0:4317 \
OTEL_PYTHON_LOG_CORRELATION=true \
OTEL_TRACES_EXPORTER=otlp \
OTEL_METRICS_EXPORTER=otlp \
OTEL_LOGS_EXPORTER=otlp \
opentelemetry-instrument python3 app1.py



# App2
OTEL_SERVICE_NAME=flask \
OTEL_EXPORTER_OTLP_ENDPOINT=http://0.0.0.0:4317 \
OTEL_PYTHON_LOG_CORRELATION=true \
OTEL_TRACES_EXPORTER=otlp \
OTEL_METRICS_EXPORTER=otlp \
OTEL_LOGS_EXPORTER=otlp \
opentelemetry-instrument python3 app2.py


python3 send_requests.py


### notes:
----
```txt
logs generating from app code
metrics, traces from python3 send_requests.py