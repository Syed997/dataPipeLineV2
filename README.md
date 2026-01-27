# DataPipelineV2

## Overview
DataPipelineV2 is a production‑grade data ingestion and anomaly detection pipeline that collects telemetry data from applications and Kubernetes infrastructure using **OpenTelemetry Collector**, performs feature engineering with **Apache Spark**, and stores the enriched data in **ClickHouse** for downstream analytics and anomaly detection.

The pipeline is designed to be modular, containerized, and easy to run locally or in a CI/CD environment. It consists of three main stages:

1. **Data Ingestion** – OpenTelemetry Collector (`otelcol-contrib`) gathers raw traces, metrics, and logs from services and exports them to Kafka.
2. **Feature Engineering** – Spark jobs read raw data from Kafka, compute sliding‑window, correlation, health, log, metric, and trace features, and write the results to CSV files under `spark/exports`.
3. **Storage** – ClickHouse stores the feature‑rich data for fast querying and anomaly detection.

## Architecture Diagram
```
+-------------------+      +-------------------+      +-------------------+
| Application / K8s | ---> | OpenTelemetry     | ---> | Kafka (topic)     |
| (OTLP)            |      | Collector (otelcol) |      | (oteloutput)      |
+-------------------+      +-------------------+      +-------------------+
                                 |
                                 v
+-------------------+      +-------------------+      +-------------------+
| Spark Streaming   | ---> | Feature Processors| ---> | ClickHouse (DB)   |
| (spark_streaming) |      | (calculate_sliding, |      | (exports)         |
|                   |      |  extract_correlation, |      |                   |
|                   |      |  extract_enhanced,   |      |                   |
|                   |      |  extract_infra_health, |      |                   |
|                   |      |  extract_log,       |      |                   |
|                   |      |  extract_metric,    |      |                   |
|                   |      |  extract_trace)     |      |                   |
+-------------------+      +-------------------+      +-------------------+
```

## Prerequisites
- **Docker** and **Docker Compose** (v2.0+)
- **Kafka** (managed via Docker Compose)
- **ClickHouse** (managed via Docker Compose)
- **OpenTelemetry Collector** binary (`otelcol-contrib_0.132.2_linux_amd64.tar.gz`)
- **Python 3.12** (for producer/consumer scripts)
- **Apache Spark** (v3.5+ – provided via `v3/` virtual environment)
- **Git** (for version control)

## Setup

### 1. Clone the repository
```bash
git clone https://github.com/Syed997/dataPipeLineV2.git
cd dataPipeLineV2
```

### 2. Build the OpenTelemetry Collector binary
```bash
tar -xzf otelcol-contrib_0.132.2_linux_amd64.tar.gz
chmod +x otelcol-contrib
```

### 3. Start the infrastructure
```bash
docker-compose up -d
```
This command brings up:
- Kafka (Zookeeper + Kafka brokers)
- ClickHouse (single node)
- Spark (via `v3/` virtual environment)

### 4. Verify services
```bash
docker ps
# Expected containers: zookeeper, kafka, clickhouse, spark (if using spark-submit)
```

## Running the Pipeline

### 1. Start OpenTelemetry Collector
```bash
./otelcol-contrib --config demo-config.yaml
```
- The collector reads from services instrumented with OpenTelemetry SDKs.
- It forwards raw data to the Kafka topic `oteloutput`.

### 2. (Optional) Produce test data
```bash
python producer/app.py
```
- Generates synthetic telemetry data and publishes to Kafka.
- also can check the apps on the experiments directory.

### 3. Run Spark feature engineering jobs
```bash
./spark/run.sh
```
- `run.sh` executes `spark/main.py` which:
  - Consumes from Kafka topics (`oteloutput`).
  - Applies the feature processors located in `spark/`.
  - Writes CSV exports to `spark/exports/`.

### 4. Verify exported files
```bash
ls spark/exports/
# Example output:
# kafka_logs_export_20250917_105635.csv
# kafka_logs_export_20250917_105836.csv
```

### 5. Load data into ClickHouse
```bash
./spark/clickhouse_setup.py
```
- Creates the necessary tables and loads CSV files from `spark/exports/` into ClickHouse.

## Configuration Files

| File | Purpose |
|------|---------|
| `demo-config.yaml` | Collector configuration (receivers, processors, exporters). |
| `docker-compose.yml` | Defines services (Kafka, ClickHouse, Spark). |
| `override-values-prod-infra.yaml` | Override values for production‑grade infrastructure (e.g., resource limits). |
| `spark/config.py` | Spark job configuration (topic names, batch intervals). |
| `spark/clickhouse_setup.py` | ClickHouse schema creation and data loading script. |
| `producer/app.py` | Example producer for testing. |
| `kafka_consumer.py` | Simple consumer for debugging. |

## Feature Engineering

The Spark job implements several feature processors:

- **Sliding Window Features** – `calculate_sliding_features_processor.py`
- **Correlation Features** – `extract_correlation_features_processor.py`
- **Enhanced Features** – `extract_enhanced_features_processor.py`
- **Infrastructure Health Features** – `extract_infra_health_features_processor.py`
- **Log Features** – `extract_log_features_processor.py`
- **Metric Features** – `extract_metric_features_processor.py`
- **Trace Features** – `extract_trace_features_processor.py`

Each processor reads raw telemetry, computes statistical aggregates, and writes a CSV file with a timestamped prefix.

## ClickHouse Storage

- **Database**: `pipeline`
- **Tables**: `features` (auto‑generated by `clickhouse_setup.py`)
- **Schema**: Each CSV column maps to a ClickHouse column (e.g., `timestamp DateTime`, `cpu_load Float64`, `error_rate Float64`, etc.)

## Anomaly Detection

The enriched data in ClickHouse can be queried for anomalies using standard SQL or integrated with external detection services (e.g., Prometheus Alertmanager, custom ML models). Example query:

```sql
SELECT *
FROM pipeline.features
WHERE error_rate > 0.05
ORDER BY timestamp DESC
LIMIT 10;
```

## Testing & Validation

- **Unit Tests**: Located in `spark/tests/` (run with `pytest`).
- **Integration Tests**: Use `kafka-test-producer.py` to emit known payloads and verify CSV output.
- **Manual Validation**: Inspect CSV files in `spark/exports/` and query ClickHouse tables.

## Contributing

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-feature`).
3. Add or modify code following the existing style (PEP‑8 for Python, consistent indentation for YAML/JSON).
4. Run `docker-compose up -d` and `./spark/run.sh` to ensure the pipeline works locally.
5. Submit a pull request with a clear description of the changes.

