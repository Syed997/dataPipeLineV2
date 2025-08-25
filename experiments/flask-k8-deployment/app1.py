from flask import Flask, jsonify
import logging
import random
import time
import threading

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

# ---- Metrics imports ----
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

# ---- Traces imports ----
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ================== LOGGING SETUP ==================
resource = Resource(attributes={"service.name": "flask"})

logger_provider = LoggerProvider(resource=resource)
otlp_log_exporter = OTLPLogExporter(endpoint="http://host.minikube.internal:4317", insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_log_exporter))
set_logger_provider(logger_provider)

handler = LoggingHandler(logger_provider=logger_provider)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)

# ================== METRICS SETUP ==================
metric_exporter = OTLPMetricExporter(endpoint="http://host.minikube.internal:4317", insecure=True)
reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=2000)

meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
from opentelemetry import metrics
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter("flask-metrics")

# Example instruments
request_counter = meter.create_counter("app_requests", description="Number of requests")
cpu_gauge = meter.create_observable_gauge(
    "app_cpu_usage",
    callbacks=[lambda options: [("usage", random.randint(1, 100))]],
    description="CPU usage %"
)

# ================== TRACES SETUP ==================
trace_exporter = OTLPSpanExporter(endpoint="http://host.minikube.internal:4317", insecure=True)
tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
from opentelemetry import trace
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer("flask-tracer")

# ================== FLASK APP ==================
app = Flask(__name__)

@app.route("/")
def home():
    logging.info("Handled / request")
    request_counter.add(1, {"route": "/"})
    return jsonify({"message": "Hello from App 1"}), 200

@app.route("/error")
def error():
    logging.error("Handled /error request")
    request_counter.add(1, {"route": "/error"})
    return jsonify({"error": "Something went wrong in App 1"}), 500

# ================== BACKGROUND GENERATORS ==================
dummy_messages = [
    "Starting process",
    "Processing request",
    "Warning: potential issue detected",
    "Error: simulated error occurred",
    "Completed successfully"
]

def generate_dummy_logs():
    while True:
        msg = random.choice(dummy_messages)
        level = random.choice([logging.INFO, logging.WARNING, logging.ERROR])
        logging.log(level=level, msg="app1_logs__:    "+msg)
        time.sleep(0.5)

def generate_dummy_metrics():
    while True:
        value = random.randint(1, 10)
        request_counter.add(value, {"source": "background"})
        level = random.choice([logging.INFO, logging.WARNING, logging.ERROR])
        logging.log(level=level, msg="app1_metrics__:    "+msg)
        time.sleep(2)

def generate_dummy_traces():
    while True:
        with tracer.start_as_current_span("background-task") as span:
            span.set_attribute("task.id", random.randint(100, 999))
            span.set_attribute("task.status", random.choice(["ok", "warn", "error"]))
            work_time = random.uniform(0.1, 0.5)
            time.sleep(work_time)
        level = random.choice([logging.INFO, logging.WARNING, logging.ERROR])
        logging.log(level=level, msg="app1_traces__:    "+msg)
        time.sleep(2)

# Start background threads
threading.Thread(target=generate_dummy_logs, daemon=True).start()
threading.Thread(target=generate_dummy_metrics, daemon=True).start()
threading.Thread(target=generate_dummy_traces, daemon=True).start()

# ================== MAIN ==================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9001)
