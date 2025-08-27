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

# Updated messages table with features column
client.execute(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.messages (
    topic String,
    value String,
    ts DateTime,
    batch_id UInt64,
    window_start DateTime,
    window_end DateTime,
    features String DEFAULT '',
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ts, topic)
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
                count(DISTINCT batch_id) as batch_count,
                countIf(features != '') as messages_with_features
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
                "batch_count": row[4],
                "messages_with_features": row[5]
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
                max(processed_at) as last_processed,
                countIf(features != '') as with_features
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
                "last_processed": stats[4].isoformat() + "Z" if stats[4] else None,
                "messages_with_features": stats[5]
            }
        else:
            return {
                "topic": topic,
                "message_count": 0,
                "time_window": "last 5 seconds",
                "earliest_message": None,
                "latest_message": None,
                "first_processed": None,
                "last_processed": None,
                "messages_with_features": 0
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
            max(ts) as latest_message,
            countIf(features != '') as messages_with_features
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
            "latest_message": row[6].isoformat() + "Z" if row[6] else None,
            "messages_with_features": row[7]
        })
    
    return {"batches": batches}

@app.get("/features/stats")
def feature_stats():
    """Get statistics about extracted features across all topics"""
    try:
        # Overall feature statistics
        result = client.execute("""
            SELECT 
                topic,
                count(*) as total_messages,
                countIf(features != '') as messages_with_features,
                countIf(JSONHas(features, 'logs')) as log_features,
                countIf(JSONHas(features, 'traces')) as trace_features,
                countIf(JSONHas(features, 'metrics')) as metric_features,
                min(ts) as earliest_message,
                max(ts) as latest_message
            FROM messages
            WHERE processed_at >= now() - INTERVAL 1 HOUR
            GROUP BY topic
            ORDER BY total_messages DESC
        """)
        
        topic_stats = []
        for row in result:
            topic_stats.append({
                "topic": row[0],
                "total_messages": row[1],
                "messages_with_features": row[2],
                "log_features": row[3],
                "trace_features": row[4],
                "metric_features": row[5],
                "earliest_message": row[6].isoformat() + "Z" if row[6] else None,
                "latest_message": row[7].isoformat() + "Z" if row[7] else None
            })
        
        # Summary statistics
        summary_result = client.execute("""
            SELECT 
                count(*) as total_messages,
                countIf(features != '') as messages_with_features,
                countIf(JSONHas(features, 'logs')) as total_log_features,
                countIf(JSONHas(features, 'traces')) as total_trace_features,
                countIf(JSONHas(features, 'metrics')) as total_metric_features,
                countDistinct(topic) as unique_topics
            FROM messages
            WHERE processed_at >= now() - INTERVAL 1 HOUR
        """)
        
        summary = {}
        if summary_result and summary_result[0]:
            stats = summary_result[0]
            summary = {
                "total_messages": stats[0],
                "messages_with_features": stats[1],
                "total_log_features": stats[2],
                "total_trace_features": stats[3],
                "total_metric_features": stats[4],
                "unique_topics": stats[5],
                "feature_extraction_rate": round(stats[1] / stats[0] * 100, 2) if stats[0] > 0 else 0
            }
        
        return {
            "time_window": "last 1 hour",
            "summary": summary,
            "topic_breakdown": topic_stats
        }
            
    except Exception as e:
        return {"error": str(e)}, 500

@app.get("/features/sample")
def feature_sample():
    """Get sample extracted features"""
    topic = request.args.get('topic')
    limit = int(request.args.get('limit', 5))
    
    query = """
        SELECT topic, features, ts
        FROM messages 
        WHERE features != '' 
        AND processed_at >= now() - INTERVAL 1 HOUR
    """
    
    params = {}
    if topic:
        query += " AND topic = %(topic)s"
        params['topic'] = topic
    
    query += f" ORDER BY ts DESC LIMIT {limit}"
    
    try:
        result = client.execute(query, params)
        
        samples = []
        for row in result:
            try:
                features_json = json.loads(row[1]) if row[1] else {}
            except json.JSONDecodeError:
                features_json = {}
            
            samples.append({
                "topic": row[0],
                "features": features_json,
                "timestamp": row[2].isoformat() + "Z"
            })
        
        return {"samples": samples}
        
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

def extract_log_features(data, message_ts):
    """Extract log features from OpenTelemetry log data"""
    features = []
    resource_logs = data.get("resourceLogs", [])
    
    for res_log in resource_logs:
        scope_logs = res_log.get("scopeLogs", [])
        for sc_log in scope_logs:
            log_records = sc_log.get("logRecords", [])
            for rec in log_records:
                observed_nano = rec.get("observedTimeUnixNano")
                observed_ts = datetime.fromtimestamp(int(observed_nano) / 1e9) if observed_nano else message_ts
                
                severity_num = rec.get("severityNumber", 0)
                severity_text = rec.get("severityText", "")
                
                body_dict = rec.get("body", {})
                body = body_dict.get("stringValue", "")
                
                trace_id = rec.get("traceId", "")
                span_id = rec.get("spanId", "")
                
                # Extract context from attributes
                context = {}
                attributes = rec.get("attributes", [])
                for attr in attributes:
                    if attr["key"] == "context":
                        context_str = attr["value"].get("stringValue", "{}")
                        try:
                            context = json.loads(context_str)
                        except json.JSONDecodeError:
                            pass
                        break
                
                # Error detection
                error_info = None
                if severity_num >= 17 or "ERROR" in body.upper():
                    error_info = {
                        "message": body,
                        "code": None
                    }
                    match = re.search(r'SQLSTATE\[(\w+)\]', body)
                    if match:
                        error_info["code"] = match.group(1)
                
                feature = {
                    "observed_ts": observed_ts.isoformat() + "Z",
                    "severity_number": severity_num,
                    "severity_text": severity_text,
                    "body": body,
                    "body_length": len(body),
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "context": context,
                    "error": error_info,
                    "hour": observed_ts.hour,
                    "day_of_week": observed_ts.weekday()
                }
                features.append(feature)
    
    return features

def extract_trace_features(data, message_ts):
    """Extract trace features from OpenTelemetry trace data"""
    features = []
    resource_spans = data.get("resourceSpans", [])
    
    for rs in resource_spans:
        # Extract resource attributes
        resource_attrs = {}
        resource_attrs_list = rs.get("resource", {}).get("attributes", [])
        for attr in resource_attrs_list:
            key = attr.get("key")
            value = attr.get("value", {})
            if key:
                resource_attrs[key] = get_attr_value(value)
        
        scope_spans = rs.get("scopeSpans", [])
        for ss in scope_spans:
            spans = ss.get("spans", [])
            for span in spans:
                trace_id = span.get("traceId", "")
                span_id = span.get("spanId", "")
                parent_span_id = span.get("parentSpanId")
                name = span.get("name", "")
                kind = span.get("kind", 0)
                
                # Extract timestamps
                start_nano = span.get("startTimeUnixNano")
                end_nano = span.get("endTimeUnixNano")
                start_time = None
                end_time = None
                duration_ms = 0
                
                if start_nano:
                    try:
                        start_time = datetime.fromtimestamp(int(start_nano) / 1e9)
                    except (ValueError, TypeError, OverflowError):
                        start_time = message_ts
                
                if end_nano:
                    try:
                        end_time = datetime.fromtimestamp(int(end_nano) / 1e9)
                    except (ValueError, TypeError, OverflowError):
                        end_time = message_ts
                
                if start_nano and end_nano:
                    try:
                        duration_ms = (int(end_nano) - int(start_nano)) / 1e6
                    except (ValueError, TypeError):
                        duration_ms = 0
                
                # Extract span attributes
                attrs = {}
                span_attrs_list = span.get("attributes", [])
                for attr in span_attrs_list:
                    key = attr.get("key")
                    value = attr.get("value", {})
                    if key:
                        attrs[key] = get_attr_value(value) or get_attr_value(value, "intValue")
                
                # Extract events and exceptions
                events = span.get("events", [])
                exception_info = None
                for event in events:
                    if event.get("name") == "exception":
                        event_attrs = {}
                        for attr in event.get("attributes", []):
                            key = attr.get("key")
                            value = attr.get("value", {})
                            if key:
                                event_attrs[key] = get_attr_value(value)
                        
                        exception_info = {
                            "type": event_attrs.get("exception.type"),
                            "message": event_attrs.get("exception.message"),
                            "stacktrace_length": len(event_attrs.get("exception.stacktrace", ""))
                        }
                        break
                
                status = span.get("status", {})
                is_error = 1 if (attrs.get("http.status_code", 0) >= 400) or status.get("code") == 2 else 0
                
                feature = {
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "parent_span_id": parent_span_id,
                    "name": name,
                    "kind": kind,
                    "start_time": start_time.isoformat() + "Z" if start_time else None,
                    "end_time": end_time.isoformat() + "Z" if end_time else None,
                    "duration_ms": duration_ms,
                    "attributes": attrs,
                    "events_count": len(events),
                    "exception": exception_info,
                    "resource_attributes": resource_attrs,
                    "is_error": is_error,
                    "hour": start_time.hour if start_time else None,
                    "day_of_week": start_time.weekday() if start_time else None
                }
                features.append(feature)
    
    return features

def extract_metric_features(data, message_ts):
    """Extract metric features from OpenTelemetry metric data"""
    features = []
    resource_metrics = data.get("resourceMetrics", [])
    
    for rm in resource_metrics:
        # Extract resource attributes
        resource_attrs = {}
        resource_attrs_list = rm.get("resource", {}).get("attributes", [])
        for attr in resource_attrs_list:
            key = attr.get("key")
            value = attr.get("value", {})
            if key:
                resource_attrs[key] = get_attr_value(value)
        
        scope_metrics = rm.get("scopeMetrics", [])
        for sm in scope_metrics:
            metrics = sm.get("metrics", [])
            for metric in metrics:
                metric_name = metric.get("name", "")
                metric_description = metric.get("description")
                metric_unit = metric.get("unit")
                
                # Process different metric types
                metric_data_points = []
                metric_type = ""
                aggregation_info = {}
                
                if "gauge" in metric:
                    metric_type = "gauge"
                    metric_data_points = metric["gauge"].get("dataPoints", [])
                elif "sum" in metric:
                    metric_type = "sum"
                    sum_data = metric["sum"]
                    metric_data_points = sum_data.get("dataPoints", [])
                    aggregation_info = {
                        "temporality": "cumulative" if sum_data.get("aggregationTemporality") == 2 else "delta",
                        "is_monotonic": bool(sum_data.get("isMonotonic"))
                    }
                elif "histogram" in metric:
                    metric_type = "histogram"
                    histogram_data = metric["histogram"]
                    metric_data_points = histogram_data.get("dataPoints", [])
                    aggregation_info = {
                        "temporality": "cumulative" if histogram_data.get("aggregationTemporality") == 2 else "delta"
                    }
                elif "exponentialHistogram" in metric:
                    metric_type = "exponentialHistogram"
                    exp_hist_data = metric["exponentialHistogram"]
                    metric_data_points = exp_hist_data.get("dataPoints", [])
                    aggregation_info = {
                        "temporality": "cumulative" if exp_hist_data.get("aggregationTemporality") == 2 else "delta"
                    }
                elif "summary" in metric:
                    metric_type = "summary"
                    metric_data_points = metric["summary"].get("dataPoints", [])
                
                # Process data points
                for dp in metric_data_points:
                    timestamp_nano = dp.get("timeUnixNano")
                    timestamp = message_ts
                    if timestamp_nano:
                        try:
                            timestamp = datetime.fromtimestamp(int(timestamp_nano) / 1e9)
                        except (ValueError, TypeError, OverflowError):
                            pass
                    
                    # Extract value
                    value = 0.0
                    if "asDouble" in dp:
                        value = dp["asDouble"]
                    elif "asInt" in dp:
                        value = float(dp["asInt"])
                    elif "count" in dp:
                        value = float(dp["count"])
                    
                    # Extract attributes
                    dp_attrs = {}
                    for attr in dp.get("attributes", []):
                        key = attr.get("key")
                        attr_value = attr.get("value", {})
                        if key:
                            dp_attrs[key] = (get_attr_value(attr_value) or 
                                           get_attr_value(attr_value, "intValue") or 
                                           get_attr_value(attr_value, "doubleValue") or 
                                           get_attr_value(attr_value, "boolValue"))
                    
                    exemplars = dp.get("exemplars", [])
                    
                    feature = {
                        "name": metric_name,
                        "type": metric_type,
                        "unit": metric_unit,
                        "description": metric_description,
                        "timestamp": timestamp.isoformat() + "Z",
                        "value": value,
                        "aggregation": aggregation_info,
                        "attributes": dp_attrs,
                        "exemplars_count": len(exemplars),
                        "resource_attributes": resource_attrs,
                        "hour": timestamp.hour,
                        "day_of_week": timestamp.weekday()
                    }
                    features.append(feature)
    
    return features

# ---------- Spark Structured Streaming job ----------
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
        
        # Process each message and extract features
        insert_data = []
        
        for r in rows:
            topic = r["topic"]
            value = r["value"]
            ts = r["ts"]
            features_json = ""
            
            # Extract features based on topic type
            try:
                data = json.loads(value)
                extracted_features = {}
                
                # Determine data type and extract features
                if "resourceLogs" in data:
                    log_features = extract_log_features(data, ts)
                    if log_features:
                        extracted_features["logs"] = log_features
                
                if "resourceSpans" in data:
                    trace_features = extract_trace_features(data, ts)
                    if trace_features:
                        extracted_features["traces"] = trace_features
                
                if "resourceMetrics" in data:
                    metric_features = extract_metric_features(data, ts)
                    if metric_features:
                        extracted_features["metrics"] = metric_features
                
                # Convert features to JSON string
                if extracted_features:
                    features_json = json.dumps(extracted_features, separators=(',', ':'))
                    
            except json.JSONDecodeError as e:
                print(f"Batch {batch_id}: Error parsing JSON for {topic}: {e}")
            except Exception as e:
                print(f"Batch {batch_id}: Unexpected error during feature extraction for {topic}: {e}")
            
            insert_data.append((
                topic,
                value,
                ts,
                batch_id,
                window_start,
                window_end,
                features_json
            ))
        
        try:
            client.execute(
                "INSERT INTO messages (topic, value, ts, batch_id, window_start, window_end, features) VALUES",
                insert_data
            )
            
            # Count processed features
            feature_counts = {
                "total": len(insert_data),
                "with_features": sum(1 for item in insert_data if item[6]),
                "logs": sum(1 for item in insert_data if '"logs":' in item[6]),
                "traces": sum(1 for item in insert_data if '"traces":' in item[6]),
                "metrics": sum(1 for item in insert_data if '"metrics":' in item[6])
            }
            
            print(f"Batch {batch_id}: Successfully inserted {len(insert_data)} records")
            print(f"Batch {batch_id}: Feature extraction - Total: {feature_counts['total']}, "
                  f"With Features: {feature_counts['with_features']}, "
                  f"Logs: {feature_counts['logs']}, "
                  f"Traces: {feature_counts['traces']}, "
                  f"Metrics: {feature_counts['metrics']}")
            
            topic_counts = {}
            for r in rows:
                topic_counts[r["topic"]] = topic_counts.get(r["topic"], 0) + 1
            print(f"Batch {batch_id}: Topic distribution: {topic_counts}")
            
        except Exception as e:
            print(f"Batch {batch_id}: Error inserting data: {e}")
            raise
        
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
    print(f"   - Feature Stats: http://localhost:{API_PORT}/features/stats")
    print(f"   - Feature Samples: http://localhost:{API_PORT}/features/sample?topic=TOPIC_NAME&limit=5")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping streaming job...")
        query.stop()

if __name__ == "__main__":
    main()