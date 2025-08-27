import os
import threading
import time
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession, functions as F, types as T
from clickhouse_driver import Client
import json
import re
import pandas as pd
import numpy as np
from collections import defaultdict, deque


BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PATTERN = os.environ.get("TOPIC_PATTERN", "demo-.*")
API_PORT = int(os.environ.get("API_PORT", "5001"))

# ---------- ClickHouse Connection ----------
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "kafka_logs")

client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

# Enhanced messages table with additional feature columns
client.execute(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.messages (
    topic String,
    value String,
    ts DateTime,
    batch_id UInt64,
    window_start DateTime,
    window_end DateTime,
    features String DEFAULT '',
    enhanced_features String DEFAULT '',
    sliding_features String DEFAULT '',
    correlation_features String DEFAULT '',
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ts, topic)
""")

# Create aggregated features table for time-series analysis
client.execute(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.aggregated_features (
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
    trend_coefficient Float64,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (window_start, topic)
""")

# Global cache for historical data and sliding window calculations
historical_cache = defaultdict(lambda: deque(maxlen=10000))  # Keep last 10k messages per topic
feature_cache = defaultdict(dict)  # Cache computed features

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
                countIf(features != '') as messages_with_features,
                countIf(enhanced_features != '') as messages_with_enhanced_features,
                countIf(sliding_features != '') as messages_with_sliding_features
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
                "messages_with_features": row[5],
                "messages_with_enhanced_features": row[6],
                "messages_with_sliding_features": row[7]
            })
        
        return {"topics": topics}
    except Exception as e:
        return {"error": str(e)}, 500

@app.get("/features/enhanced")
def enhanced_feature_stats():
    """Get statistics about enhanced features across all topics"""
    try:
        result = client.execute("""
            SELECT 
                topic,
                count(*) as total_messages,
                countIf(enhanced_features != '') as enhanced_count,
                countIf(sliding_features != '') as sliding_count,
                countIf(correlation_features != '') as correlation_count,
                avg(JSONExtractFloat(enhanced_features, 'messages_per_minute')) as avg_msg_rate,
                avg(JSONExtractFloat(enhanced_features, 'error_rate')) as avg_error_rate,
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
                "enhanced_count": row[2],
                "sliding_count": row[3],
                "correlation_count": row[4],
                "avg_msg_rate": round(row[5], 2) if row[5] else 0,
                "avg_error_rate": round(row[6], 4) if row[6] else 0,
                "earliest_message": row[7].isoformat() + "Z" if row[7] else None,
                "latest_message": row[8].isoformat() + "Z" if row[8] else None
            })
        
        return {
            "time_window": "last 1 hour",
            "topic_breakdown": topic_stats
        }
            
    except Exception as e:
        return {"error": str(e)}, 500

@app.get("/trends/<topic>")
def get_trends(topic):
    """Get trend analysis for a specific topic"""
    try:
        result = client.execute("""
            SELECT 
                window_start,
                messages_per_minute,
                error_rate,
                trend_coefficient,
                avg_duration_ms
            FROM aggregated_features
            WHERE topic = %(topic)s
            AND window_start >= now() - INTERVAL 6 HOUR
            ORDER BY window_start DESC
            LIMIT 50
        """, {'topic': topic})
        
        trends = []
        for row in result:
            trends.append({
                "window_start": row[0].isoformat() + "Z",
                "messages_per_minute": round(row[1], 2) if row[1] else 0,
                "error_rate": round(row[2], 4) if row[2] else 0,
                "trend_coefficient": round(row[3], 6) if row[3] else 0,
                "avg_duration_ms": round(row[4], 2) if row[4] else 0
            })
        
        return {"topic": topic, "trends": trends}
        
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

def calculate_trend(values):
    """Calculate linear trend coefficient using least squares"""
    if len(values) < 2:
        return 0.0
    
    values = np.array(values, dtype=float)
    if np.all(np.isnan(values)) or len(values) == 0:
        return 0.0
    
    # Remove NaN values
    valid_values = values[~np.isnan(values)]
    if len(valid_values) < 2:
        return 0.0
    
    x = np.arange(len(valid_values))
    try:
        # Calculate slope using least squares
        slope = np.polyfit(x, valid_values, 1)[0]
        return float(slope)
    except (np.linalg.LinAlgError, ValueError):
        return 0.0
    
def normalize_datetime(dt):
    """Normalize datetime to UTC timezone-aware datetime"""
    if dt is None:
        return None
    
    if isinstance(dt, str):
        try:
            # Try parsing ISO format with timezone
            if dt.endswith('Z'):
                dt = dt[:-1] + '+00:00'
            dt = datetime.fromisoformat(dt)
        except ValueError:
            try:
                # Try parsing without timezone
                dt = datetime.fromisoformat(dt)
            except ValueError:
                return None
    
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            # Assume UTC if no timezone info
            dt = dt.replace(tzinfo=timezone.utc)
        elif dt.tzinfo != timezone.utc:
            # Convert to UTC
            dt = dt.astimezone(timezone.utc)
    
    return dt

def datetime_to_naive_utc(dt):
    """Convert datetime to naive UTC datetime for consistent comparisons"""
    dt = normalize_datetime(dt)
    if dt is None:
        return None
    return dt.replace(tzinfo=None)


def extract_enhanced_features(messages, window_minutes=5):
    """Extract time-based aggregation features with proper datetime handling"""
    if not messages:
        return {}
    
    # Convert messages to DataFrame with proper datetime handling
    processed_messages = []
    for msg in messages:
        processed_msg = msg.copy()
        # Normalize datetime fields
        if 'timestamp' in processed_msg:
            processed_msg['timestamp'] = datetime_to_naive_utc(processed_msg['timestamp'])
        if 'observed_ts' in processed_msg:
            processed_msg['observed_ts'] = datetime_to_naive_utc(processed_msg['observed_ts'])
        processed_messages.append(processed_msg)
    
    df = pd.DataFrame(processed_messages)
    
    # Basic rate calculations
    rate_features = {
        'messages_per_minute': len(df) / max(window_minutes, 1),
        'unique_sources': df.get('source', pd.Series()).nunique() if 'source' in df.columns else 0,
        'window_minutes': window_minutes
    }
    
    # Error rate calculation
    if 'severity_number' in df.columns:
        rate_features['error_rate'] = df['severity_number'].ge(17).mean()
    elif 'is_error' in df.columns:
        rate_features['error_rate'] = df['is_error'].mean()
    else:
        rate_features['error_rate'] = 0.0
    
    # Statistical features for numerical columns
    numerical_cols = ['body_length', 'duration_ms', 'value']
    for col in numerical_cols:
        if col in df.columns and not df[col].empty:
            col_data = pd.to_numeric(df[col], errors='coerce').dropna()
            if len(col_data) > 0:
                rate_features.update({
                    f'{col}_mean': float(col_data.mean()),
                    f'{col}_std': float(col_data.std()) if len(col_data) > 1 else 0.0,
                    f'{col}_p95': float(col_data.quantile(0.95)),
                    f'{col}_cv': float(col_data.std() / col_data.mean()) if col_data.mean() > 0 else 0.0,
                    f'{col}_min': float(col_data.min()),
                    f'{col}_max': float(col_data.max())
                })
    
    # Time distribution features
    if 'hour' in df.columns and not df['hour'].empty:
        rate_features['peak_hour'] = int(df['hour'].mode().iloc[0])
        rate_features['hour_variance'] = float(df['hour'].var()) if len(df['hour']) > 1 else 0.0
    
    return rate_features

def calculate_sliding_features(topic, current_batch_data, window_sizes=[5, 15, 60]):
    """Calculate sliding window features using cached historical data with proper datetime handling"""
    features = {}
    current_time = datetime.now(timezone.utc).replace(tzinfo=None)  # Naive UTC
    
    # Get historical data from cache
    historical_data = list(historical_cache[topic])
    
    if not historical_data:
        return features
    
    # Process historical data with proper datetime handling
    processed_data = []
    for item in historical_data:
        processed_item = item.copy()
        if 'timestamp' in processed_item:
            processed_item['timestamp'] = datetime_to_naive_utc(processed_item['timestamp'])
        processed_data.append(processed_item)
    
    # Convert to DataFrame
    df = pd.DataFrame(processed_data)
    if 'timestamp' not in df.columns or df['timestamp'].isna().all():
        return features
    
    # Ensure timestamp column is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    for window in window_sizes:
        # Get data from last N minutes
        cutoff = current_time - timedelta(minutes=window)
        window_data = df[df['timestamp'] >= cutoff]
        
        if len(window_data) == 0:
            features.update({
                f'avg_messages_{window}m': 0.0,
                f'error_rate_{window}m': 0.0,
                f'trend_{window}m': 0.0
            })
            continue
        
        features[f'avg_messages_{window}m'] = len(window_data) / max(window, 1)
        
        # Error rate calculation
        if 'severity_number' in window_data.columns:
            features[f'error_rate_{window}m'] = (window_data['severity_number'] >= 17).mean()
        elif 'is_error' in window_data.columns:
            features[f'error_rate_{window}m'] = window_data['is_error'].mean()
        else:
            features[f'error_rate_{window}m'] = 0.0
        
        # Trend detection
        if 'value' in window_data.columns and len(window_data) > 1:
            values = pd.to_numeric(window_data['value'], errors='coerce').dropna().values
            features[f'trend_{window}m'] = calculate_trend(values)
        else:
            features[f'trend_{window}m'] = 0.0
        
        # Additional sliding features
        if len(window_data) > 1:
            # Message volume trend
            window_data_sorted = window_data.sort_values('timestamp')
            time_buckets = pd.cut(window_data_sorted['timestamp'], bins=5, labels=False)
            bucket_counts = window_data_sorted.groupby(time_buckets).size().values
            features[f'volume_trend_{window}m'] = calculate_trend(bucket_counts)
            
            # Response time trend (if duration exists)
            if 'duration_ms' in window_data.columns:
                duration_data = pd.to_numeric(window_data['duration_ms'], errors='coerce').dropna().values
                if len(duration_data) > 0:
                    features[f'duration_trend_{window}m'] = calculate_trend(duration_data)
                    features[f'avg_duration_{window}m'] = float(np.mean(duration_data))
    
    return features


def extract_infra_health_features(metrics_data):
    """Extract infrastructure health features from Kubernetes metrics"""
    health_features = {}
    
    if not metrics_data:
        return health_features
    
    # Convert to DataFrame for easier processing
    df = pd.DataFrame(metrics_data)
    
    # Look for common Kubernetes metric patterns
    k8s_metrics = {
        'pods_restarting': 0,
        'memory_pressure': 0,
        'resource_utilization': 0.0,
        'unhealthy_pods': 0,
        'cpu_throttling': 0.0,
        'disk_pressure': 0
    }
    
    for metric in metrics_data:
        name = metric.get('name', '')
        value = metric.get('value', 0)
        attributes = metric.get('attributes', {})
        
        # Map metric names to health indicators
        if 'container.restarts' in name or 'pod.restart' in name:
            k8s_metrics['pods_restarting'] += int(value) if value > 0 else 0
        elif 'node.condition.memory_pressure' in name:
            k8s_metrics['memory_pressure'] = max(k8s_metrics['memory_pressure'], int(value))
        elif 'container.cpu.request' in name or 'cpu.utilization' in name:
            k8s_metrics['resource_utilization'] += float(value)
        elif 'container.ready' in name:
            if value == 0:
                k8s_metrics['unhealthy_pods'] += 1
        elif 'cpu.throttled' in name:
            k8s_metrics['cpu_throttling'] += float(value)
        elif 'node.condition.disk_pressure' in name:
            k8s_metrics['disk_pressure'] = max(k8s_metrics['disk_pressure'], int(value))
    
    health_features.update(k8s_metrics)
    return health_features

def extract_correlation_features(logs, metrics, traces):
    """Extract correlation features across different telemetry signals"""
    correlations = {}
    
    # Error correlation across signals
    log_errors = sum(1 for log in logs if log.get('error') or log.get('severity_number', 0) >= 17)
    trace_errors = sum(1 for trace in traces if trace.get('is_error') or trace.get('exception'))
    metric_anomalies = sum(1 for metric in metrics if metric.get('value', 0) > 1000)  # Simple anomaly threshold
    
    total_logs = len(logs)
    total_traces = len(traces)
    total_metrics = len(metrics)
    
    correlations.update({
        'error_correlation_score': log_errors * trace_errors if log_errors > 0 and trace_errors > 0 else 0,
        'error_to_request_ratio': log_errors / max(total_traces, 1),
        'error_density': (log_errors + trace_errors) / max(total_logs + total_traces, 1),
        'metric_anomaly_rate': metric_anomalies / max(total_metrics, 1),
        'cross_signal_health_score': 1.0 - min(1.0, (log_errors + trace_errors + metric_anomalies) / max(total_logs + total_traces + total_metrics, 1))
    })
    
    # Service dependency correlation
    trace_services = set()
    for trace in traces:
        attrs = trace.get('attributes', {})
        service_name = attrs.get('service.name')
        if service_name:
            trace_services.add(service_name)
    
    correlations['service_diversity'] = len(trace_services)
    
    # Time correlation features
    if logs and traces:
        log_times = [log.get('observed_ts') for log in logs if log.get('observed_ts')]
        trace_times = [trace.get('start_time') for trace in traces if trace.get('start_time')]
        
        if log_times and trace_times:
            # Simple time overlap detection
            correlations['temporal_overlap'] = 1 if len(set(log_times) & set(trace_times)) > 0 else 0
    
    return correlations

def update_historical_cache(topic, message_data):
    """Update the historical cache with new message data - ensuring proper datetime handling"""
    # Create a safe copy of the message data for caching
    cache_entry = {}
    
    # Handle datetime fields properly
    if 'timestamp' in message_data:
        cache_entry['timestamp'] = datetime_to_naive_utc(message_data['timestamp'])
    elif 'observed_ts' in message_data:
        cache_entry['timestamp'] = datetime_to_naive_utc(message_data['observed_ts'])
    else:
        cache_entry['timestamp'] = datetime.now(timezone.utc).replace(tzinfo=None)
    
    # Copy other safe fields
    safe_fields = ['severity_number', 'is_error', 'value', 'duration_ms', 'body_length', 'source', 'hour']
    for field in safe_fields:
        if field in message_data:
            value = message_data[field]
            # Ensure JSON serializable types
            if isinstance(value, (int, float, str, bool, type(None))):
                cache_entry[field] = value
            else:
                cache_entry[field] = str(value)
    
    # Set defaults for missing fields
    cache_entry.setdefault('severity_number', 0)
    cache_entry.setdefault('is_error', 0)
    cache_entry.setdefault('value', 0)
    cache_entry.setdefault('duration_ms', 0)
    cache_entry.setdefault('body_length', 0)
    cache_entry.setdefault('source', '')
    cache_entry.setdefault('hour', cache_entry['timestamp'].hour if cache_entry['timestamp'] else 0)
    
    historical_cache[topic].append(cache_entry)

def get_recent_messages(topic, window_minutes=5):
    """Get recent messages from cache for feature extraction with proper datetime handling"""
    cutoff_time = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=window_minutes)
    historical_data = list(historical_cache[topic])
    
    recent_messages = []
    for msg in historical_data:
        msg_time = msg.get('timestamp')
        if msg_time is None:
            continue
            
        # Normalize the message time
        msg_time = datetime_to_naive_utc(msg_time)
        if msg_time is None:
            continue
        
        if msg_time >= cutoff_time:
            recent_messages.append(msg)
    
    return recent_messages


def make_json_serializable(obj):
    """Convert objects to JSON serializable format"""
    if isinstance(obj, datetime):
        return obj.isoformat() + 'Z' if obj.tzinfo is None else obj.isoformat()
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [make_json_serializable(item) for item in obj]
    else:
        return obj

# Original feature extraction functions (keeping existing ones)
def extract_log_features(data, message_ts):
    """Extract log features from OpenTelemetry log data with proper datetime handling"""
    features = []
    resource_logs = data.get("resourceLogs", [])
    
    # Normalize message timestamp
    message_ts = normalize_datetime(message_ts)
    if message_ts is None:
        message_ts = datetime.now(timezone.utc)
    
    for res_log in resource_logs:
        scope_logs = res_log.get("scopeLogs", [])
        for sc_log in scope_logs:
            log_records = sc_log.get("logRecords", [])
            for rec in log_records:
                observed_nano = rec.get("observedTimeUnixNano")
                observed_ts = message_ts
                
                if observed_nano:
                    try:
                        observed_ts = datetime.fromtimestamp(int(observed_nano) / 1e9, tz=timezone.utc)
                    except (ValueError, TypeError, OverflowError):
                        observed_ts = message_ts
                
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
                    "observed_ts": observed_ts.isoformat(),
                    "severity_number": severity_num,
                    "severity_text": severity_text,
                    "body": body,
                    "body_length": len(body),
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "context": context,
                    "error": error_info,
                    "hour": observed_ts.hour,
                    "day_of_week": observed_ts.weekday(),
                    "timestamp": observed_ts.isoformat(),
                    "value": severity_num,  # For trend analysis
                    "source": context.get('service.name', 'unknown')
                }
                
                # Make sure all values are JSON serializable
                feature = make_json_serializable(feature)
                features.append(feature)
    
    return features

def extract_trace_features(data, message_ts):
    """Extract trace features from OpenTelemetry trace data with proper datetime handling"""
    features = []
    resource_spans = data.get("resourceSpans", [])
    
    # Normalize message timestamp
    message_ts = normalize_datetime(message_ts)
    if message_ts is None:
        message_ts = datetime.now(timezone.utc)
    
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
                
                # Extract timestamps with proper timezone handling
                start_nano = span.get("startTimeUnixNano")
                end_nano = span.get("endTimeUnixNano")
                start_time = message_ts
                end_time = message_ts
                duration_ms = 0
                
                if start_nano:
                    try:
                        start_time = datetime.fromtimestamp(int(start_nano) / 1e9, tz=timezone.utc)
                    except (ValueError, TypeError, OverflowError):
                        start_time = message_ts
                
                if end_nano:
                    try:
                        end_time = datetime.fromtimestamp(int(end_nano) / 1e9, tz=timezone.utc)
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
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "duration_ms": duration_ms,
                    "attributes": attrs,
                    "events_count": len(events),
                    "exception": exception_info,
                    "resource_attributes": resource_attrs,
                    "is_error": is_error,
                    "hour": start_time.hour,
                    "day_of_week": start_time.weekday(),
                    "timestamp": start_time.isoformat(),
                    "value": duration_ms,  # For trend analysis
                    "source": attrs.get('service.name', resource_attrs.get('service.name', 'unknown'))
                }
                
                # Make sure all values are JSON serializable
                feature = make_json_serializable(feature)
                features.append(feature)
    
    return features

def extract_metric_features(data, message_ts):
    """Extract metric features from OpenTelemetry metric data with proper datetime handling"""
    features = []
    resource_metrics = data.get("resourceMetrics", [])
    
    # Normalize message timestamp
    message_ts = normalize_datetime(message_ts)
    if message_ts is None:
        message_ts = datetime.now(timezone.utc)
    
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
                            timestamp = datetime.fromtimestamp(int(timestamp_nano) / 1e9, tz=timezone.utc)
                        except (ValueError, TypeError, OverflowError):
                            timestamp = message_ts
                    
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
                        "timestamp": timestamp.isoformat(),
                        "value": value,
                        "aggregation": aggregation_info,
                        "attributes": dp_attrs,
                        "exemplars_count": len(exemplars),
                        "resource_attributes": resource_attrs,
                        "hour": timestamp.hour,
                        "day_of_week": timestamp.weekday(),
                        "source": resource_attrs.get('service.name', 'unknown')
                    }
                    
                    # Make sure all values are JSON serializable
                    feature = make_json_serializable(feature)
                    features.append(feature)
    
    return features
# ---------- Enhanced Spark Structured Streaming job ----------
def save_to_clickhouse_enhanced(batch_df, batch_id: int):
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
    
    # Process each message and extract all types of features
    insert_data = []
    aggregated_features = []
    
    # Group data by topic for better feature extraction
    topic_data = defaultdict(list)
    for r in rows:
        topic_data[r["topic"]].append(r)
    
    for topic, topic_rows in topic_data.items():
        print(f"Batch {batch_id}: Processing {len(topic_rows)} messages for topic {topic}")
        
        # Collect all features for correlation analysis
        all_logs = []
        all_traces = []
        all_metrics = []
        
        for r in topic_rows:
            value = r["value"]
            ts = r["ts"]
            features_json = ""
            enhanced_features_json = ""
            sliding_features_json = ""
            correlation_features_json = ""
            
            # Extract original features based on topic type
            try:
                data = json.loads(value)
                extracted_features = {}
                
                # Determine data type and extract features
                if "resourceLogs" in data:
                    log_features = extract_log_features(data, ts)
                    if log_features:
                        extracted_features["logs"] = log_features
                        all_logs.extend(log_features)
                        # Update cache with log data
                        for log_feature in log_features:
                            update_historical_cache(topic, log_feature)
                
                if "resourceSpans" in data:
                    trace_features = extract_trace_features(data, ts)
                    if trace_features:
                        extracted_features["traces"] = trace_features
                        all_traces.extend(trace_features)
                        # Update cache with trace data
                        for trace_feature in trace_features:
                            update_historical_cache(topic, trace_feature)
                
                if "resourceMetrics" in data:
                    metric_features = extract_metric_features(data, ts)
                    if metric_features:
                        extracted_features["metrics"] = metric_features
                        all_metrics.extend(metric_features)
                        # Update cache with metric data
                        for metric_feature in metric_features:
                            update_historical_cache(topic, metric_feature)
                
                # Convert original features to JSON string
                if extracted_features:
                    features_json = json.dumps(extracted_features, separators=(',', ':'))
                    
            except json.JSONDecodeError as e:
                print(f"Batch {batch_id}: Error parsing JSON for {topic}: {e}")
                continue
            except Exception as e:
                print(f"Batch {batch_id}: Unexpected error during feature extraction for {topic}: {e}")
                continue
            
            # Extract enhanced time-based features
            try:
                recent_messages = get_recent_messages(topic, window_minutes=5)
                if recent_messages:
                    enhanced_features = extract_enhanced_features(recent_messages, window_minutes=5)
                    if enhanced_features:
                        enhanced_features_json = json.dumps(enhanced_features, separators=(',', ':'))
            except Exception as e:
                print(f"Batch {batch_id}: Error extracting enhanced features for {topic}: {e}")
            
            # Extract sliding window features
            try:
                sliding_features = calculate_sliding_features(topic, batch_df, window_sizes=[5, 15, 60])
                if sliding_features:
                    sliding_features_json = json.dumps(sliding_features, separators=(',', ':'))
            except Exception as e:
                print(f"Batch {batch_id}: Error extracting sliding features for {topic}: {e}")
            
            insert_data.append((
                topic,
                value,
                ts,
                batch_id,
                window_start,
                window_end,
                features_json,
                enhanced_features_json,
                sliding_features_json,
                ""  # correlation_features will be added after processing all messages
            ))
        
        # Extract correlation features across all signals for this topic
        try:
            if all_logs or all_traces or all_metrics:
                correlation_features = extract_correlation_features(all_logs, all_metrics, all_traces)
                
                # Add infrastructure health features if we have metrics
                if all_metrics:
                    infra_features = extract_infra_health_features(all_metrics)
                    correlation_features.update(infra_features)
                
                correlation_features_json = json.dumps(correlation_features, separators=(',', ':'))
                
                # Update correlation features for all messages in this topic
                for i, (topic_name, value, ts, batch_id_val, ws, we, feat, enh_feat, slide_feat, _) in enumerate(insert_data):
                    if topic_name == topic:
                        insert_data[i] = (topic_name, value, ts, batch_id_val, ws, we, feat, enh_feat, slide_feat, correlation_features_json)
        
        except Exception as e:
            print(f"Batch {batch_id}: Error extracting correlation features for {topic}: {e}")
        
        # Create aggregated features for time-series analysis
        try:
            recent_messages = get_recent_messages(topic, window_minutes=5)
            if recent_messages:
                enhanced_features = extract_enhanced_features(recent_messages, window_minutes=5)
                
                # Calculate trend coefficient
                values = [msg.get('value', 0) for msg in recent_messages]
                trend_coeff = calculate_trend(values) if len(values) > 1 else 0.0
                
                aggregated_feature = (
                    topic,
                    window_start,
                    window_end,
                    5,  # window_minutes
                    enhanced_features.get('messages_per_minute', 0.0),
                    enhanced_features.get('error_rate', 0.0),
                    enhanced_features.get('unique_sources', 0),
                    enhanced_features.get('body_length_mean', 0.0),
                    enhanced_features.get('duration_ms_mean', 0.0),
                    enhanced_features.get('duration_ms_p95', 0.0),
                    trend_coeff
                )
                aggregated_features.append(aggregated_feature)
        except Exception as e:
            print(f"Batch {batch_id}: Error creating aggregated features for {topic}: {e}")
    
    # Insert main message data
    try:
        if insert_data:
            client.execute(
                "INSERT INTO messages (topic, value, ts, batch_id, window_start, window_end, features, enhanced_features, sliding_features, correlation_features) VALUES",
                insert_data
            )
            print(f"Batch {batch_id}: Successfully inserted {len(insert_data)} message records")
        
        # Insert aggregated features
        if aggregated_features:
            client.execute(
                "INSERT INTO aggregated_features (topic, window_start, window_end, window_minutes, messages_per_minute, error_rate, unique_sources, avg_body_length, avg_duration_ms, p95_duration_ms, trend_coefficient) VALUES",
                aggregated_features
            )
            print(f"Batch {batch_id}: Successfully inserted {len(aggregated_features)} aggregated feature records")
        
        # Count processed features
        feature_counts = {
            "total": len(insert_data),
            "with_features": sum(1 for item in insert_data if item[6]),  # original features
            "with_enhanced": sum(1 for item in insert_data if item[7]),  # enhanced features
            "with_sliding": sum(1 for item in insert_data if item[8]),   # sliding features
            "with_correlation": sum(1 for item in insert_data if item[9]), # correlation features
            "logs": sum(1 for item in insert_data if '"logs":' in item[6]),
            "traces": sum(1 for item in insert_data if '"traces":' in item[6]),
            "metrics": sum(1 for item in insert_data if '"metrics":' in item[6])
        }
        
        print(f"Batch {batch_id}: Feature extraction summary:")
        print(f"  - Total messages: {feature_counts['total']}")
        print(f"  - With original features: {feature_counts['with_features']}")
        print(f"  - With enhanced features: {feature_counts['with_enhanced']}")
        print(f"  - With sliding features: {feature_counts['with_sliding']}")
        print(f"  - With correlation features: {feature_counts['with_correlation']}")
        print(f"  - Logs: {feature_counts['logs']}, Traces: {feature_counts['traces']}, Metrics: {feature_counts['metrics']}")
        
        # Topic distribution
        topic_counts = {}
        for item in insert_data:
            topic_counts[item[0]] = topic_counts.get(item[0], 0) + 1
        print(f"Batch {batch_id}: Topic distribution: {topic_counts}")
        
    except Exception as e:
        print(f"Batch {batch_id}: Error inserting data: {e}")
        raise

def main():
    """Main function to start the enhanced streaming job"""
    threading.Thread(target=run_api, daemon=True).start()
    
    spark = (SparkSession.builder
             .appName("EnhancedKafkaToClickHouse")
             .config("spark.sql.streaming.metricsEnabled", "true")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")  # Handle topic deletion gracefully
        .load())
    
    events = df.select(
        F.col("topic").cast("string").alias("topic"),
        F.col("value").cast("string").alias("value"),
        F.col("timestamp").alias("ts")
    )
    
    # Use the enhanced save function
    query = (events.writeStream
        .outputMode("append")
        .foreachBatch(save_to_clickhouse_enhanced)
        .option("checkpointLocation", "/tmp/spark-clickhouse-enhanced-checkpoints")
        .trigger(processingTime="5 seconds")
        .start())
    
    print("🚀 Enhanced Streaming job started!")
    print(f"📊 Processing data every 5 seconds from topics: {TOPIC_PATTERN}")
    print(f"💾 Saving to ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}")
    print(f"🔍 Enhanced API endpoints available:")
    print(f"   - Health: http://localhost:{API_PORT}/health")
    print(f"   - Topics: http://localhost:{API_PORT}/topics")
    print(f"   - Enhanced Features: http://localhost:{API_PORT}/features/enhanced")
    print(f"   - Trends: http://localhost:{API_PORT}/trends/<topic>")
    print(f"   - Count: http://localhost:{API_PORT}/count?topic=TOPIC_NAME")
    print(f"   - Batches: http://localhost:{API_PORT}/batches/recent")
    print(f"   - Feature Stats: http://localhost:{API_PORT}/features/stats")
    print(f"   - Feature Samples: http://localhost:{API_PORT}/features/sample?topic=TOPIC_NAME&limit=5")
    print(f"📈 New features include:")
    print(f"   - Time-based aggregations (messages/min, error rates)")
    print(f"   - Sliding window analysis (5m, 15m, 1h)")
    print(f"   - Infrastructure health metrics")
    print(f"   - Cross-signal correlations")
    print(f"   - Trend analysis and anomaly detection")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping enhanced streaming job...")
        query.stop()
    finally:
        # Clean up cache
        historical_cache.clear()
        feature_cache.clear()
        print("🧹 Cache cleaned up")

if __name__ == "__main__":
    main()