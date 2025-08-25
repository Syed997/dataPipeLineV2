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

# Initialize LoggerProvider with service name
resource = Resource(attributes={"service.name": "flask"})
logger_provider = LoggerProvider(resource=resource)

# Configure OTLP Log Exporter
otlp_exporter = OTLPLogExporter(endpoint="http://0.0.0.0:4317", insecure=True)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(otlp_exporter))

# Set the LoggerProvider
set_logger_provider(logger_provider)

# Attach OTLP handler to the root logger
handler = LoggingHandler(logger_provider=logger_provider)
logging.getLogger().addHandler(handler)

# Flask application setup
app = Flask(__name__)

@app.route("/")
def home():
    logging.info("Handled / request")
    return jsonify({"message": "Hello from App 1"}), 200

@app.route("/error")
def error():
    logging.error("Handled /error request")
    return jsonify({"error": "Something went wrong in App 1"}), 500

# Background thread to generate dummy logs
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
        logging.log(level, msg)
        time.sleep(0.5)  #

threading.Thread(target=generate_dummy_logs, daemon=True).start()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=9002)
