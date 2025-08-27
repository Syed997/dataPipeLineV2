import os
import threading
import time
from datetime import datetime
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession, functions as F, types as T
from clickhouse_driver import Client
import json
import re

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PATTERN = os.environ.get("TOPIC_PATTERN", "demo-.*")
API_PORT = int(os.environ.get("API_PORT", "5001"))

# ---------- ClickHouse Connection ----------
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "kafka_logs")

client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

# Existing messages table
client.execute(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.messages (
    topic String,
    value String,
    ts DateTime,
    batch_id UInt64,
    window_start DateTime,
    window_end DateTime,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ts, topic)
""")

# Existing log_features table
client.execute(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.log_features (
    observed_ts DateTime64,
    severity_number UInt8,
    severity_text String,
    body String,
    body_length UInt32,
    trace_id String,
    span_id String,
    url Nullable(String),
    method Nullable(String),
    ip Nullable(String),
    user_agent Nullable(String),
    status_code Nullable(UInt16),
    response_data_length Nullable(UInt64),
    error_message Nullable(String),
    error_code Nullable(String),
    hour UInt8,
    day_of_week UInt8,
    batch_id UInt64,
    window_start DateTime,
    window_end DateTime,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (observed_ts, trace_id)
""")

# New trace_features table
client.execute(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.trace_features (
    trace_id String,
    span_id String,
    parent_span_id Nullable(String),
    name String,
    kind UInt8,
    start_time DateTime64,
    end_time DateTime64,
    duration_ms Float64,
    http_method Nullable(String),
    http_url Nullable(String),
    http_status_code Nullable(UInt16),
    user_agent Nullable(String),
    client_ip Nullable(String),
    code_function Nullable(String),
    code_namespace Nullable(String),
    code_filepath Nullable(String),
    code_lineno Nullable(UInt32),
    url_scheme Nullable(String),
    network_protocol_version Nullable(String),
    network_peer_address Nullable(String),
    server_address Nullable(String),
    server_port Nullable(UInt16),
    client_port Nullable(String),
    http_route Nullable(String),
    status_code Nullable(UInt8),
    event_count UInt32,
    has_exception UInt8,
    exception_type Nullable(String),
    exception_message Nullable(String),
    exception_stacktrace_length UInt32,
    service_name String,
    service_version String,
    host_name String,
    os_type String,
    is_error UInt8,
    hour Nullable(UInt8),
    day_of_week Nullable(UInt8),
    batch_id UInt64,
    window_start DateTime,
    window_end DateTime,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (start_time, trace_id, span_id)
""")

# New metric_features table
client.execute(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.metric_features (
    metric_name String,
    metric_type String,
    metric_unit Nullable(String),
    metric_description Nullable(String),
    timestamp DateTime64,
    value Float64,
    aggregation_temporality Nullable(String),
    is_monotonic Nullable(UInt8),
    attribute_keys Array(String),
    attribute_values Array(String),
    service_name String,
    service_version String,
    host_name String,
    os_type String,
    exemplar_count UInt32,
    has_exemplars UInt8,
    hour UInt8,
    day_of_week UInt8,
    batch_id UInt64,
    window_start DateTime,
    window_end DateTime,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (timestamp, metric_name, service_name)
""")

# ---------- Flask API ----------
app = Flask(__name__)

@app.get("/health")
def health():
    return {"status": "ok", "server_time_utc": datetime.utcnow().isoformat() + "Z"}

@app.get("/topics")
def list_topics():
    try:
        result = client.execute("""
            SELECT 
                topic,
                count(*) as total_messages,
                min(ts) as first_message,
                max(ts) as last_message,
                count(DISTINCT batch_id) as batch_count
            FROM messages 
            GROUP BY topic
            ORDER BY total_messages DESC
        """)
        
        topics = []
        for row in result:
            topics.append({
                "topic": row[0],
                "total_messages": row[1],
                "first_message": row[2].isoformat() + "Z" if row[2] else None,
                "last_message": row[3].isoformat() + "Z" if row[3] else None,
                "batch_count": row[4]
            })
        
        return {"topics": topics}
    except Exception as e:
        return {"error": str(e)}, 500

@app.get("/count")
def count_messages():
    topic = request.args.get('topic')
    if not topic:
        return {"error": "Topic parameter is required"}, 400
    
    try:
        result = client.execute("""
            SELECT count(*) as message_count
            FROM messages 
            WHERE topic = %(topic)s 
            AND processed_at >= now() - INTERVAL 5 SECOND
        """, {'topic': topic})
        
        count = result[0][0] if result else 0
        
        stats_result = client.execute("""
            SELECT 
                count(*) as total_count,
                min(ts) as earliest_message,
                max(ts) as latest_message,
                min(processed_at) as first_processed,
                max(processed_at) as last_processed
            FROM messages 
            WHERE topic = %(topic)s 
            AND processed_at >= now() - INTERVAL 5 SECOND
        """, {'topic': topic})
        
        if stats_result and stats_result[0][0] > 0:
            stats = stats_result[0]
            return {
                "topic": topic,
                "message_count": count,
                "time_window": "last 5 seconds",
                "earliest_message": stats[1].isoformat() + "Z" if stats[1] else None,
                "latest_message": stats[2].isoformat() + "Z" if stats[2] else None,
                "first_processed": stats[3].isoformat() + "Z" if stats[3] else None,
                "last_processed": stats[4].isoformat() + "Z" if stats[4] else None
            }
        else:
            return {
                "topic": topic,
                "message_count": 0,
                "time_window": "last 5 seconds",
                "earliest_message": None,
                "latest_message": None,
                "first_processed": None,
                "last_processed": None
            }
    except Exception as e:
        return {"error": str(e)}, 500

@app.get("/batches/recent")
def recent_batches():
    result = client.execute("""
        SELECT 
            batch_id,
            window_start,
            window_end,
            count(*) as message_count,
            countDistinct(topic) as topic_count,
            min(ts) as earliest_message,
            max(ts) as latest_message
        FROM messages 
        WHERE processed_at >= now() - INTERVAL 1 HOUR
        GROUP BY batch_id, window_start, window_end
        ORDER BY batch_id DESC
        LIMIT 10
    """)
    
    batches = []
    for row in result:
        batches.append({
            "batch_id": row[0],
            "window_start": row[1].isoformat() + "Z",
            "window_end": row[2].isoformat() + "Z",
            "message_count": row[3],
            "topic_count": row[4],
            "earliest_message": row[5].isoformat() + "Z" if row[5] else None,
            "latest_message": row[6].isoformat() + "Z" if row[6] else None
        })
    
    return {"batches": batches}

@app.get("/features/log_stats")
def log_feature_stats():
    """Get statistics about extracted log features"""
    try:
        result = client.execute("""
            SELECT 
                count(*) as total_features,
                countDistinct(trace_id) as unique_traces,
                countDistinct(error_code) as unique_error_codes,
                avg(body_length) as avg_body_length,
                countIf(error_message IS NOT NULL) as error_count,
                min(observed_ts) as earliest_feature,
                max(observed_ts) as latest_feature
            FROM log_features
            WHERE processed_at >= now() - INTERVAL 1 HOUR
        """)
        
        if result and result[0]:
            stats = result[0]
            return {
                "total_features": stats[0],
                "unique_traces": stats[1],
                "unique_error_codes": stats[2],
                "avg_body_length": round(stats[3], 2) if stats[3] else 0,
                "error_count": stats[4],
                "earliest_feature": stats[5].isoformat() + "Z" if stats[5] else None,
                "latest_feature": stats[6].isoformat() + "Z" if stats[6] else None,
                "time_window": "last 1 hour"
            }
        else:
            return {
                "total_features": 0,
                "unique_traces": 0,
                "unique_error_codes": 0,
                "avg_body_length": 0,
                "error_count": 0,
                "earliest_feature": None,
                "latest_feature": None,
                "time_window": "last 1 hour"
            }
            
    except Exception as e:
        return {"error": str(e)}, 500

@app.get("/features/trace_stats")
def trace_feature_stats():
    """Get statistics about extracted trace features"""
    try:
        result = client.execute("""
            SELECT 
                count(*) as total_spans,
                countDistinct(trace_id) as unique_traces,
                avg(duration_ms) as avg_duration_ms,
                countIf(is_error = 1) as error_count,
                countIf(has_exception = 1) as exception_count,
                countDistinct(service_name) as unique_services,
                min(start_time) as earliest_span,
                max(end_time) as latest_span
            FROM trace_features
            WHERE processed_at >= now() - INTERVAL 1 HOUR
        """)
        
        if result and result[0]:
            stats = result[0]
            return {
                "total_spans": stats[0],
                "unique_traces": stats[1],
                "avg_duration_ms": round(stats[2], 2) if stats[2] else 0,
                "error_count": stats[3],
                "exception_count": stats[4],
                "unique_services": stats[5],
                "earliest_span": stats[6].isoformat() + "Z" if stats[6] else None,
                "latest_span": stats[7].isoformat() + "Z" if stats[7] else None,
                "time_window": "last 1 hour"
            }
        else:
            return {
                "total_spans": 0,
                "unique_traces": 0,
                "avg_duration_ms": 0,
                "error_count": 0,
                "exception_count": 0,
                "unique_services": 0,
                "earliest_span": None,
                "latest_span": None,
                "time_window": "last 1 hour"
            }
            
    except Exception as e:
        return {"error": str(e)}, 500

@app.get("/features/metric_stats")
def metric_feature_stats():
    """Get statistics about extracted metric features"""
    try:
        result = client.execute("""
            SELECT 
                count(*) as total_metrics,
                countDistinct(metric_name) as unique_metric_names,
                countDistinct(service_name) as unique_services,
                avg(value) as avg_value,
                countIf(has_exemplars = 1) as metrics_with_exemplars,
                countDistinct(metric_type) as unique_metric_types,
                min(timestamp) as earliest_metric,
                max(timestamp) as latest_metric
            FROM metric_features
            WHERE processed_at >= now() - INTERVAL 1 HOUR
        """)
        
        if result and result[0]:
            stats = result[0]
            return {
                "total_metrics": stats[0],
                "unique_metric_names": stats[1],
                "unique_services": stats[2],
                "avg_value": round(stats[3], 2) if stats[3] else 0,
                "metrics_with_exemplars": stats[4],
                "unique_metric_types": stats[5],
                "earliest_metric": stats[6].isoformat() + "Z" if stats[6] else None,
                "latest_metric": stats[7].isoformat() + "Z" if stats[7] else None,
                "time_window": "last 1 hour"
            }
        else:
            return {
                "total_metrics": 0,
                "unique_metric_names": 0,
                "unique_services": 0,
                "avg_value": 0,
                "metrics_with_exemplars": 0,
                "unique_metric_types": 0,
                "earliest_metric": None,
                "latest_metric": None,
                "time_window": "last 1 hour"
            }
            
    except Exception as e:
        return {"error": str(e)}, 500

def run_api():
    app.run(host="0.0.0.0", port=API_PORT)

# Helper function to safely extract attribute values
def get_attr_value(attr_dict, expected_type="stringValue"):
    """Safely extract attribute value with type checking"""
    if not isinstance(attr_dict, dict):
        return None
    return attr_dict.get(expected_type)

# ---------- Spark Structured Streaming job with Windowing ----------
def main():
    threading.Thread(target=run_api, daemon=True).start()
    
    spark = (SparkSession.builder
             .appName("KafkaToClickHouse")
             .config("spark.sql.streaming.metricsEnabled", "true")
             .getOrCreate())
    
    df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "latest")
        .load())
    
    events = df.select(
        F.col("topic").cast("string").alias("topic"),
        F.col("value").cast("string").alias("value"),
        F.col("timestamp").alias("ts")
    )
    
    def save_to_clickhouse(batch_df, batch_id: int):
        print(f"Processing batch {batch_id} at {datetime.utcnow()}")
        
        rows = batch_df.collect()
        if not rows:
            print(f"Batch {batch_id}: No data to process")
            return
            
        timestamps = [r["ts"] for r in rows]
        window_start = min(timestamps)
        window_end = max(timestamps)
        
        print(f"Batch {batch_id}: Processing {len(rows)} messages")
        print(f"Batch {batch_id}: Time window {window_start} to {window_end}")
        
        # Insert raw messages
        insert_data = [
            (
                r["topic"],
                r["value"], 
                r["ts"],
                batch_id,
                window_start,
                window_end
            )
            for r in rows
        ]
        
        try:
            client.execute(
                "INSERT INTO messages (topic, value, ts, batch_id, window_start, window_end) VALUES",
                insert_data
            )
            print(f"Batch {batch_id}: Successfully inserted {len(insert_data)} records")
            
            topic_counts = {}
            for r in rows:
                topic_counts[r["topic"]] = topic_counts.get(r["topic"], 0) + 1
            print(f"Batch {batch_id}: Topic distribution: {topic_counts}")
        except Exception as e:
            print(f"Batch {batch_id}: Error inserting data: {e}")
            raise
        
        # Log feature extraction for demo-flask-logs
        log_feature_data = []
        laravel_log_rows = [r for r in rows if r["topic"] == "demo-flask-logs"]
        
        for r in laravel_log_rows:
            try:
                data = json.loads(r["value"])
                resource_logs = data.get("resourceLogs", [])
                for res_log in resource_logs:
                    scope_logs = res_log.get("scopeLogs", [])
                    for sc_log in scope_logs:
                        log_records = sc_log.get("logRecords", [])
                        for rec in log_records:
                            observed_nano = rec.get("observedTimeUnixNano")
                            observed_ts = datetime.fromtimestamp(int(observed_nano) / 1e9) if observed_nano else r["ts"]
                            
                            severity_num = rec.get("severityNumber", 0)
                            severity_text = rec.get("severityText", "")
                            
                            body_dict = rec.get("body", {})
                            body = body_dict.get("stringValue", "")
                            body_length = len(body)
                            
                            trace_id = rec.get("traceId", "")
                            span_id = rec.get("spanId", "")
                            
                            url = method = ip = user_agent = status_code = response_data_len = error_msg = error_code = None
                            
                            attributes = rec.get("attributes", [])
                            for attr in attributes:
                                if attr["key"] == "context":
                                    context_str = attr["value"].get("stringValue", "{}")
                                    try:
                                        context = json.loads(context_str)
                                        url = context.get("url")
                                        method = context.get("method")
                                        ip = context.get("ip")
                                        user_agent = context.get("user_agent")
                                        status_code = context.get("status_code")
                                        response_data = context.get("response_data")
                                        if response_data is not None:
                                            response_data_len = len(str(response_data))
                                    except json.JSONDecodeError:
                                        pass
                                    break
                            
                            if severity_num >= 17 or "ERROR" in body.upper():
                                error_msg = body
                                match = re.search(r'SQLSTATE\[(\w+)\]', body)
                                if match:
                                    error_code = match.group(1)
                            
                            hour = observed_ts.hour
                            day_of_week = observed_ts.weekday()
                            
                            log_feature_data.append((
                                observed_ts,
                                severity_num,
                                severity_text,
                                body,
                                body_length,
                                trace_id,
                                span_id,
                                url,
                                method,
                                ip,
                                user_agent,
                                status_code,
                                response_data_len,
                                error_msg,
                                error_code,
                                hour,
                                day_of_week,
                                batch_id,
                                window_start,
                                window_end
                            ))
            except json.JSONDecodeError as e:
                print(f"Batch {batch_id}: Error parsing JSON for demo-flask-logs: {e}")
            except Exception as e:
                print(f"Batch {batch_id}: Unexpected error during log feature extraction: {e}")
        
        if log_feature_data:
            try:
                client.execute(
                    f"INSERT INTO {CLICKHOUSE_DB}.log_features (observed_ts, severity_number, severity_text, body, body_length, trace_id, span_id, url, method, ip, user_agent, status_code, response_data_length, error_message, error_code, hour, day_of_week, batch_id, window_start, window_end) VALUES",
                    log_feature_data
                )
                print(f"Batch {batch_id}: Successfully inserted {len(log_feature_data)} log features")
            except Exception as e:
                print(f"Batch {batch_id}: Error inserting log features: {e}")
        
        # Trace feature extraction for demo-flask-traces
        trace_feature_data = []
        laravel_trace_rows = [r for r in rows if r["topic"] == "demo-flask-traces"]
        
        for r in laravel_trace_rows:
            try:
                data = json.loads(r["value"])
                resource_spans = data.get("resourceSpans", [])
                for rs in resource_spans:
                    # Extract resource attributes safely
                    resource_attrs = {}
                    resource_attrs_list = rs.get("resource", {}).get("attributes", [])
                    for attr in resource_attrs_list:
                        key = attr.get("key")
                        value = attr.get("value", {})
                        if key:
                            resource_attrs[key] = value
                    
                    scope_spans = rs.get("scopeSpans", [])
                    for ss in scope_spans:
                        spans = ss.get("spans", [])
                        for span in spans:
                            trace_id = span.get("traceId", "")
                            span_id = span.get("spanId", "")
                            parent_span_id = span.get("parentSpanId")
                            name = span.get("name", "")
                            kind = span.get("kind", 0)
                            
                            start_nano = span.get("startTimeUnixNano")
                            end_nano = span.get("endTimeUnixNano")
                            start_time = None
                            end_time = None
                            duration_ms = 0
                            
                            if start_nano:
                                try:
                                    start_time = datetime.fromtimestamp(int(start_nano) / 1e9)
                                except (ValueError, TypeError, OverflowError):
                                    start_time = r["ts"]  # fallback to message timestamp
                            
                            if end_nano:
                                try:
                                    end_time = datetime.fromtimestamp(int(end_nano) / 1e9)
                                except (ValueError, TypeError, OverflowError):
                                    end_time = r["ts"]  # fallback to message timestamp
                            
                            if start_nano and end_nano:
                                try:
                                    duration_ms = (int(end_nano) - int(start_nano)) / 1e6
                                except (ValueError, TypeError):
                                    duration_ms = 0
                            
                            # Extract span attributes safely
                            attrs = {}
                            span_attrs_list = span.get("attributes", [])
                            for attr in span_attrs_list:
                                key = attr.get("key")
                                value = attr.get("value", {})
                                if key:
                                    attrs[key] = value
                            
                            # Extract HTTP and other attributes with safe defaults
                            http_method = get_attr_value(attrs.get("http.method", {}))
                            http_url = get_attr_value(attrs.get("http.url", {}))
                            http_status_code = get_attr_value(attrs.get("http.status_code", {}), "intValue")
                            user_agent = get_attr_value(attrs.get("http.user_agent", {}))
                            client_ip = get_attr_value(attrs.get("http.client_ip", {}))
                            code_function = get_attr_value(attrs.get("code.function", {}))
                            code_namespace = get_attr_value(attrs.get("code.namespace", {}))
                            code_filepath = get_attr_value(attrs.get("code.filepath", {}))
                            code_lineno = get_attr_value(attrs.get("code.lineno", {}), "intValue")
                            url_scheme = get_attr_value(attrs.get("url.scheme", {}))
                            network_protocol_version = get_attr_value(attrs.get("network.protocol.version", {}))
                            network_peer_address = get_attr_value(attrs.get("network.peer.address", {}))
                            server_address = get_attr_value(attrs.get("server.address", {}))
                            server_port = get_attr_value(attrs.get("server.port", {}), "intValue")
                            client_port = get_attr_value(attrs.get("client.port", {}))
                            http_route = get_attr_value(attrs.get("http.route", {}))
                            
                            status = span.get("status", {})
                            status_code = status.get("code")
                            
                            events = span.get("events", [])
                            event_count = len(events)
                            has_exception = 0
                            exception_type = None
                            exception_message = None
                            exception_stacktrace_length = 0
                            
                            for event in events:
                                if event.get("name") == "exception":
                                    has_exception = 1
                                    event_attrs = {}
                                    event_attrs_list = event.get("attributes", [])
                                    for attr in event_attrs_list:
                                        key = attr.get("key")
                                        value = attr.get("value", {})
                                        if key:
                                            event_attrs[key] = value
                                    
                                    exception_type = get_attr_value(event_attrs.get("exception.type", {}))
                                    exception_message = get_attr_value(event_attrs.get("exception.message", {}))
                                    stacktrace = get_attr_value(event_attrs.get("exception.stacktrace", {}), "stringValue")
                                    exception_stacktrace_length = len(stacktrace) if stacktrace else 0
                                    break
                            
                            # Extract resource attributes with safe defaults
                            service_name = get_attr_value(resource_attrs.get("service.name", {}), "stringValue") or ""
                            service_version = get_attr_value(resource_attrs.get("service.version", {}), "stringValue") or ""
                            host_name = get_attr_value(resource_attrs.get("host.name", {}), "stringValue") or ""
                            os_type = get_attr_value(resource_attrs.get("os.type", {}), "stringValue") or ""
                            
                            is_error = 1 if (http_status_code and http_status_code >= 400) or status_code == 2 else 0
                            hour = start_time.hour if start_time else None
                            day_of_week = start_time.weekday() if start_time else None
                            
                            trace_feature_data.append((
                                trace_id,
                                span_id,
                                parent_span_id,
                                name,
                                kind,
                                start_time,
                                end_time,
                                duration_ms,
                                http_method,
                                http_url,
                                http_status_code,
                                user_agent,
                                client_ip,
                                code_function,
                                code_namespace,
                                code_filepath,
                                code_lineno,
                                url_scheme,
                                network_protocol_version,
                                network_peer_address,
                                server_address,
                                server_port,
                                client_port,
                                http_route,
                                status_code,
                                event_count,
                                has_exception,
                                exception_type,
                                exception_message,
                                exception_stacktrace_length,
                                service_name,
                                service_version,
                                host_name,
                                os_type,
                                is_error,
                                hour,
                                day_of_week,
                                batch_id,
                                window_start,
                                window_end
                            ))
            except json.JSONDecodeError as e:
                print(f"Batch {batch_id}: Error parsing JSON for demo-flask-traces: {e}")
            except Exception as e:
                print(f"Batch {batch_id}: Unexpected error during trace feature extraction: {e}")
        
        if trace_feature_data:
            try:
                client.execute(
                    f"INSERT INTO {CLICKHOUSE_DB}.trace_features (trace_id, span_id, parent_span_id, name, kind, start_time, end_time, duration_ms, http_method, http_url, http_status_code, user_agent, client_ip, code_function, code_namespace, code_filepath, code_lineno, url_scheme, network_protocol_version, network_peer_address, server_address, server_port, client_port, http_route, status_code, event_count, has_exception, exception_type, exception_message, exception_stacktrace_length, service_name, service_version, host_name, os_type, is_error, hour, day_of_week, batch_id, window_start, window_end) VALUES",
                    trace_feature_data
                )
                print(f"Batch {batch_id}: Successfully inserted {len(trace_feature_data)} trace features")
            except Exception as e:
                print(f"Batch {batch_id}: Error inserting trace features: {e}")
        
        # Metric feature extraction for demo-flask-metrics
        metric_feature_data = []
        laravel_metric_rows = [r for r in rows if r["topic"] == "demo-flask-metrics"]
        
        for r in laravel_metric_rows:
            try:
                data = json.loads(r["value"])
                resource_metrics = data.get("resourceMetrics", [])
                for rm in resource_metrics:
                    # Extract resource attributes safely
                    resource_attrs = {}
                    resource_attrs_list = rm.get("resource", {}).get("attributes", [])
                    for attr in resource_attrs_list:
                        key = attr.get("key")
                        value = attr.get("value", {})
                        if key:
                            resource_attrs[key] = value
                    
                    scope_metrics = rm.get("scopeMetrics", [])
                    for sm in scope_metrics:
                        metrics = sm.get("metrics", [])
                        for metric in metrics:
                            metric_name = metric.get("name", "")
                            metric_description = metric.get("description")
                            metric_unit = metric.get("unit")
                            
                            # Extract resource attributes with safe defaults
                            service_name = get_attr_value(resource_attrs.get("service.name", {}), "stringValue") or ""
                            service_version = get_attr_value(resource_attrs.get("service.version", {}), "stringValue") or ""
                            host_name = get_attr_value(resource_attrs.get("host.name", {}), "stringValue") or ""
                            os_type = get_attr_value(resource_attrs.get("os.type", {}), "stringValue") or ""
                            
                            # Process different metric types
                            metric_data_points = []
                            metric_type = ""
                            aggregation_temporality = None
                            is_monotonic = None
                            
                            # Handle Gauge metrics
                            if "gauge" in metric:
                                metric_type = "gauge"
                                metric_data_points = metric["gauge"].get("dataPoints", [])
                            
                            # Handle Sum metrics
                            elif "sum" in metric:
                                metric_type = "sum"
                                sum_data = metric["sum"]
                                metric_data_points = sum_data.get("dataPoints", [])
                                aggregation_temporality = "cumulative" if sum_data.get("aggregationTemporality") == 2 else "delta"
                                is_monotonic = 1 if sum_data.get("isMonotonic") else 0
                            
                            # Handle Histogram metrics
                            elif "histogram" in metric:
                                metric_type = "histogram"
                                histogram_data = metric["histogram"]
                                metric_data_points = histogram_data.get("dataPoints", [])
                                aggregation_temporality = "cumulative" if histogram_data.get("aggregationTemporality") == 2 else "delta"
                            
                            # Handle ExponentialHistogram metrics
                            elif "exponentialHistogram" in metric:
                                metric_type = "exponentialHistogram"
                                exp_hist_data = metric["exponentialHistogram"]
                                metric_data_points = exp_hist_data.get("dataPoints", [])
                                aggregation_temporality = "cumulative" if exp_hist_data.get("aggregationTemporality") == 2 else "delta"
                            
                            # Handle Summary metrics
                            elif "summary" in metric:
                                metric_type = "summary"
                                metric_data_points = metric["summary"].get("dataPoints", [])
                            
                            # Process data points
                            for dp in metric_data_points:
                                # Extract timestamp
                                timestamp_nano = dp.get("timeUnixNano")
                                timestamp = None
                                if timestamp_nano:
                                    try:
                                        timestamp = datetime.fromtimestamp(int(timestamp_nano) / 1e9)
                                    except (ValueError, TypeError, OverflowError):
                                        timestamp = r["ts"]  # fallback to message timestamp
                                else:
                                    timestamp = r["ts"]
                                
                                # Extract value based on metric type
                                value = 0.0
                                if metric_type in ["gauge", "sum"]:
                                    # For gauge and sum, get the numeric value
                                    if "asDouble" in dp:
                                        value = dp["asDouble"]
                                    elif "asInt" in dp:
                                        value = float(dp["asInt"])
                                elif metric_type == "histogram":
                                    # For histogram, use count as the value
                                    value = float(dp.get("count", 0))
                                elif metric_type == "exponentialHistogram":
                                    # For exponential histogram, use count as the value
                                    value = float(dp.get("count", 0))
                                elif metric_type == "summary":
                                    # For summary, use count as the value
                                    value = float(dp.get("count", 0))
                                
                                # Extract attributes
                                attribute_keys = []
                                attribute_values = []
                                dp_attrs = dp.get("attributes", [])
                                for attr in dp_attrs:
                                    key = attr.get("key")
                                    attr_value = attr.get("value", {})
                                    if key and attr_value:
                                        attribute_keys.append(key)
                                        # Convert attribute value to string
                                        if "stringValue" in attr_value:
                                            attribute_values.append(attr_value["stringValue"])
                                        elif "intValue" in attr_value:
                                            attribute_values.append(str(attr_value["intValue"]))
                                        elif "doubleValue" in attr_value:
                                            attribute_values.append(str(attr_value["doubleValue"]))
                                        elif "boolValue" in attr_value:
                                            attribute_values.append(str(attr_value["boolValue"]))
                                        else:
                                            attribute_values.append("")
                                
                                # Check for exemplars
                                exemplars = dp.get("exemplars", [])
                                exemplar_count = len(exemplars)
                                has_exemplars = 1 if exemplar_count > 0 else 0
                                
                                # Derived timestamp features
                                hour = timestamp.hour if timestamp else 0
                                day_of_week = timestamp.weekday() if timestamp else 0
                                
                                metric_feature_data.append((
                                    metric_name,
                                    metric_type,
                                    metric_unit,
                                    metric_description,
                                    timestamp,
                                    value,
                                    aggregation_temporality,
                                    is_monotonic,
                                    attribute_keys,
                                    attribute_values,
                                    service_name,
                                    service_version,
                                    host_name,
                                    os_type,
                                    exemplar_count,
                                    has_exemplars,
                                    hour,
                                    day_of_week,
                                    batch_id,
                                    window_start,
                                    window_end
                                ))
                                
            except json.JSONDecodeError as e:
                print(f"Batch {batch_id}: Error parsing JSON for demo-flask-metrics: {e}")
            except Exception as e:
                print(f"Batch {batch_id}: Unexpected error during metric feature extraction: {e}")
        
        if metric_feature_data:
            try:
                client.execute(
                    f"INSERT INTO {CLICKHOUSE_DB}.metric_features (metric_name, metric_type, metric_unit, metric_description, timestamp, value, aggregation_temporality, is_monotonic, attribute_keys, attribute_values, service_name, service_version, host_name, os_type, exemplar_count, has_exemplars, hour, day_of_week, batch_id, window_start, window_end) VALUES",
                    metric_feature_data
                )
                print(f"Batch {batch_id}: Successfully inserted {len(metric_feature_data)} metric features")
            except Exception as e:
                print(f"Batch {batch_id}: Error inserting metric features: {e}")
        
    query = (events.writeStream
        .outputMode("append")
        .foreachBatch(save_to_clickhouse)
        .option("checkpointLocation", "/tmp/spark-clickhouse-checkpoints")
        .trigger(processingTime="5 seconds")
        .start())
    
    print("🚀 Streaming job started!")
    print(f"📊 Processing data every 5 seconds from topics: {TOPIC_PATTERN}")
    print(f"💾 Saving to ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}")
    print(f"🔍 API endpoints available:")
    print(f"   - Health: http://localhost:{API_PORT}/health")
    print(f"   - Count: http://localhost:{API_PORT}/count?topic=TOPIC_NAME")
    print(f"   - Topics: http://localhost:{API_PORT}/topics")
    print(f"   - Batches: http://localhost:{API_PORT}/batches/recent")
    print(f"   - Log Features: http://localhost:{API_PORT}/features/log_stats")
    print(f"   - Trace Features: http://localhost:{API_PORT}/features/trace_stats")
    print(f"   - Metric Features: http://localhost:{API_PORT}/features/metric_stats")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping streaming job...")
        query.stop()

if __name__ == "__main__":
    main()