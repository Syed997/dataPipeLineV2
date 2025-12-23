# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**DataPipeLineV2** is an OpenTelemetry (OTEL) data processing pipeline that ingests observability signals (traces, logs, metrics) from distributed applications, processes them using Apache Spark Structured Streaming and Apache Flink, and stores enriched features in ClickHouse for real-time analytics.

**Data Flow**: Applications (Flask/Laravel/Node.js) → OpenTelemetry Collector → Kafka Topics → Stream Processors (Spark/Flink) → ClickHouse → REST API

## Development Commands

### Environment Setup & Deployment

```bash
# Start all services (Kafka, Spark, ClickHouse, Flink, OTEL Collector)
docker compose up -d --build

# View logs for specific service
docker compose logs -f spark
docker compose logs -f jobmanager
docker compose logs -f otel-collector

# Stop all services
docker compose down

# Remove volumes (clean state)
docker compose down -v
```

### Running the Producer (Test Data Generator)

The producer service is not in docker-compose. Run it manually if needed:

```bash
cd producer
pip install -r requirements.txt
python app.py

# Start producing to default topics (demo-alpha, demo-beta, demo-gamma)
curl -X POST http://localhost:5000/start

# Start producing to custom topics (must match TOPIC_PATTERN in Spark)
curl -X POST http://localhost:5000/start \
  -H "Content-Type: application/json" \
  -d '{"topics": ["test-app1-traces", "test-app2-logs"]}'

# Check producer status
curl http://localhost:5000/status

# Stop producing
curl -X POST http://localhost:5000/stop
```

### Spark Streaming API Endpoints

Spark exposes a REST API on port 5001:

```bash
# Health check
curl http://localhost:5001/health

# List all topics being consumed
curl http://localhost:5001/topics

# Get message count for a topic
curl "http://localhost:5001/count?topic=test-flask-logs"

# Get enhanced features for a topic
curl "http://localhost:5001/features/enhanced?topic=test-flask-logs"

# Get trend analysis (window_minutes defaults to 5)
curl "http://localhost:5001/trends/test-flask-logs?window_minutes=15"

# Get feature statistics across all topics
curl http://localhost:5001/features/stats

# Get recent batch information
curl http://localhost:5001/batches/recent
```

Spark UI available at: http://localhost:4040 (while job is running)

### Flink Job Submission

Flink runs in session mode with JobManager UI at http://localhost:8081

```bash
# Submit a PyFlink job to the cluster
docker exec -it jobmanager flink run \
  --python /project/jobs/demo_logs_processing_job.py \
  --jarfile /project/flink-sql-connector-kafka-3.3.0-1.20.jar

# Submit the simple word count example
docker exec -it jobmanager flink run \
  --python /project/jobs/simple_pyflink_job.py \
  --jarfile /project/flink-sql-connector-kafka-3.3.0-1.20.jar

# List running jobs
docker exec -it jobmanager flink list

# Cancel a job (get JOB_ID from list command or Web UI)
docker exec -it jobmanager flink cancel <JOB_ID>
```

**Note**: Flink projects are mounted as volumes at `/project`, so you can edit jobs locally without rebuilding the image.

### ClickHouse Database Access

```bash
# Connect to ClickHouse CLI
docker exec -it clickhouse clickhouse-client

# Query messages table
SELECT topic, COUNT(*) as count, MIN(ts) as earliest, MAX(ts) as latest
FROM kafka_logs.messages
GROUP BY topic
ORDER BY count DESC;

# Query aggregated features
SELECT topic, window_start, messages_per_minute, error_rate
FROM kafka_logs.aggregated_features
WHERE topic = 'test-flask-logs'
ORDER BY window_start DESC
LIMIT 10;

# Check CSV exports (generated every 2 minutes by Spark)
ls -lh spark/exports/
```

ClickHouse HTTP interface: http://localhost:8123

### Kafka Management

```bash
# List all topics
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

# Describe a topic
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test-flask-logs

# Consume messages from a topic (from beginning)
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-flask-logs \
  --from-beginning \
  --max-messages 10

# Delete a topic (if needed)
docker exec -it kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic test-old-topic
```

Kafka is configured with auto-topic creation enabled (`KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true`).

### OpenTelemetry Collector

The OTEL Collector receives signals from instrumented applications and routes them to Kafka topics.

**Receiver Endpoints**:
- OTLP gRPC: `http://localhost:54317`
- OTLP HTTP: `http://localhost:4318`

**Configuration**: `demo-config.yaml` (mounted into the container)

To modify OTEL Collector behavior, edit `demo-config.yaml` and restart:
```bash
docker compose restart otel-collector
```

## Architecture Details

### Kafka Topic Naming Convention

Topics are organized by source application and signal type:

- **Pattern**: `{environment}-{service}-{signal_type}`
- **Examples**:
  - `test-flask-traces`, `test-flask-logs`, `test-flask-metrics`
  - `test-laravel-traces`, `test-laravel-logs`, `test-laravel-metrics`
  - `test-nodejs-traces`, `test-nodejs-logs`, `test-nodejs-metrics`

Spark subscribes using regex pattern `test-.*` (configurable via `TOPIC_PATTERN` env var).

### Spark Processing Pipeline

**Entry Point**: `spark/main.py`

The Spark application performs three concurrent operations:
1. **Streaming Job** (`spark_streaming.py`): Consumes from Kafka, extracts features, writes to ClickHouse
2. **REST API Server** (`api.py`): Provides query endpoints (Flask on port 5001)
3. **CSV Export Worker**: Exports data to `./exports/` every 2 minutes

**Batch Processing** (triggered every 5 seconds):
1. Read messages from Kafka topics matching pattern
2. Deserialize OTLP JSON payloads
3. Extract features using specialized processors:
   - `extract_log_features_processor.py` - Log severity, body, errors, trace IDs
   - `extract_trace_features_processor.py` - Span duration, exceptions, attributes
   - `extract_metric_features_processor.py` - Gauge, sum, histogram metrics
   - `extract_enhanced_features_processor.py` - Statistical aggregations (mean, std, p95)
   - `calculate_sliding_features_processor.py` - Multi-window aggregations (5m, 15m, 1h)
   - `extract_correlation_features_processor.py` - Cross-signal correlations
   - `extract_infra_health_features_processor.py` - Infrastructure metrics
4. Write to ClickHouse tables:
   - `messages` - Raw messages with extracted features
   - `aggregated_features` - Pre-computed window aggregations

**Key Configuration** (in `docker-compose.yml`):
- `BOOTSTRAP_SERVERS`: Kafka broker address (default: `kafka:9092`)
- `TOPIC_PATTERN`: Regex for topic subscription (default: `test-.*`)
- `CLICKHOUSE_HOST/PORT`: Database connection
- `API_PORT`: Flask API port (default: `5001`)

### Flink Integration

Flink is configured in **session mode** with separate JobManager and TaskManager containers.

**Project Structure**:
- `flink/jobs/` - PyFlink job definitions
- `flink/operators/` - Operator abstractions (currently unused)
- `flink/operator_functions/` - Operator implementations
- `flink/metric_feature_extractor.py` - Shared metric extraction logic
- `flink/utils.py` - Common utilities (datetime parsing, JSON serialization)

**Active Jobs**:
- `demo_logs_processing_job.py` - Kafka→Kafka log enrichment (adds `processed_by_flink` flag)
- `simple_pyflink_job.py` - Basic Kafka-to-Kafka pipeline example
- `simple.py` - Word count streaming example

**Development Workflow**:
1. Edit job files locally in `flink/jobs/`
2. Submit to cluster using `flink run --python` (no rebuild needed due to volume mounts)
3. Monitor execution in Web UI: http://localhost:8081

### ClickHouse Schema

**Database**: `kafka_logs`

**Table 1: `messages`** (raw storage with features)
```sql
CREATE TABLE kafka_logs.messages (
    topic String,
    value String,              -- Raw OTLP JSON
    ts DateTime,
    batch_id UInt64,
    window_start DateTime,
    window_end DateTime,
    features String,           -- JSON: signal-specific features
    enhanced_features String,  -- JSON: statistical aggregations
    sliding_features String,   -- JSON: windowed metrics
    correlation_features String, -- JSON: cross-signal correlations
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ts, topic);
```

**Table 2: `aggregated_features`** (pre-computed analytics)
```sql
CREATE TABLE kafka_logs.aggregated_features (
    topic String,
    window_start DateTime,
    window_end DateTime,
    window_minutes UInt32,
    messages_per_minute Float64,
    error_rate Float64,
    unique_sources UInt32,
    avg_body_length Float64,
    avg_duration_ms Float64,
    p95_duration_ms Float64,
    trend_coefficient Float64,  -- Slope of time-series regression
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (window_start, topic);
```

Setup script: `spark/clickhouse_setup.py` (automatically run on Spark startup)

### Feature Extraction Strategy

**Signal-Specific Features**:
- **Logs**: severity_number, severity_text, body, body_length, trace_id, span_id, error detection, SQLSTATE codes
- **Traces**: span_duration_ms, exception_count, exception_details, parent_span_id, span attributes, span status
- **Metrics**: metric_type (gauge/sum/histogram/summary), aggregation_temporality, data_point values, resource attributes

**Enhanced Features** (time-series analytics):
- Messages per minute rate
- Error rate percentage
- Statistical measures: mean, std, coefficient of variation, p95
- Peak hour detection

**Sliding Features** (multi-window aggregations):
- 5-minute, 15-minute, 1-hour rolling windows
- Count, sum, avg, std, p50, p95 per window

**Correlation Features**:
- Cross-signal analysis (e.g., correlating log errors with trace exceptions)
- Temporal alignment of related signals

### Recent Development (feature/salekeen/flink-integration)

Current branch focuses on integrating Apache Flink as an alternative stream processor:

**Completed**:
- Flink session cluster setup with JobManager/TaskManager
- PyFlink job framework with Kafka connectors
- `demo_logs_processing_job.py` for log enrichment
- Operator abstraction layer (`operators/`, `operator_functions/`)
- Volume-mounted project files for fast iteration

**Notable Commits**:
- `ffb44a8` - Fixed detector → operator terminology
- `c56a2df` - Defined operator interface and structure
- `4733750` - Added demo_logs_processing_job.py
- `c56a2df` - Kafka connection setup successful

## Service Ports Reference

| Service | Port(s) | Purpose |
|---------|---------|---------|
| Kafka | 9092 (internal), 9094 (external) | Message broker |
| Spark API | 5001 | REST API for queries |
| Spark UI | 4040 | Spark job monitoring |
| ClickHouse | 9000 (native), 8123 (HTTP) | Database access |
| Flink JobManager | 8081 | Web UI & job submission |
| OTEL Collector | 54317 (gRPC), 4318 (HTTP) | Signal ingestion |
| Producer | 5000 | Test data generator API (not in docker-compose) |

## Important Notes

### Spark Streaming Behavior
- Spark uses `subscribePattern` to automatically discover new topics matching the regex
- Batch trigger interval: 5 seconds (configurable in `spark_streaming.py`)
- Rolling message cache: Last 5 minutes per topic (for sliding window calculations)
- CSV exports written to `./spark/exports/` with timestamps

### Kafka Auto-Topic Creation
- Kafka creates topics on-demand when producers/consumers reference them
- Default partition count: 1 (can be adjusted in Kafka config)
- Replication factor: 1 (single broker setup)

### ClickHouse Performance
- Uses MergeTree engine for efficient time-series queries
- Primary key: `(ts, topic)` for messages, `(window_start, topic)` for aggregated_features
- Consider partitioning by toYYYYMM(ts) for very large datasets

### Flink vs Spark Usage
- **Spark**: Primary processor for comprehensive feature extraction and analytics
- **Flink**: Specialized jobs for specific transformations (e.g., log enrichment, metric preprocessing)
- Both can run concurrently, processing different topics or applying different logic

### Testing with Instrumented Applications
For testing with real OTEL data (not the producer):
1. Instrument your Flask/Laravel/Node.js app with OpenTelemetry SDK
2. Configure OTLP exporter to point to `http://localhost:54317` (gRPC) or `http://localhost:4318` (HTTP)
3. OTEL Collector will route signals to appropriate Kafka topics
4. Spark/Flink will automatically consume and process the data

See `experiments/flask/`, `experiments/laravel/`, and `experiments/flask-k8-deployment/` for instrumentation examples.