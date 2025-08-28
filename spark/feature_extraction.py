import json
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from utils import normalize_datetime, datetime_to_naive_utc, calculate_trend, make_json_serializable, get_attr_value
from cache import get_recent_messages, historical_cache

def extract_log_features(data, message_ts):
    features = []
    resource_logs = data.get("resourceLogs", [])
    
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
                    "value": severity_num,
                    "source": context.get('service.name', 'unknown')
                }
                
                feature = make_json_serializable(feature)
                features.append(feature)
    
    return features

def extract_trace_features(data, message_ts):
    features = []
    resource_spans = data.get("resourceSpans", [])
    
    message_ts = normalize_datetime(message_ts)
    if message_ts is None:
        message_ts = datetime.now(timezone.utc)
    
    for rs in resource_spans:
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
                
                attrs = {}
                span_attrs_list = span.get("attributes", [])
                for attr in span_attrs_list:
                    key = attr.get("key")
                    value = attr.get("value", {})
                    if key:
                        attrs[key] = get_attr_value(value) or get_attr_value(value, "intValue")
                
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
                    "value": duration_ms,
                    "source": attrs.get('service.name', resource_attrs.get('service.name', 'unknown'))
                }
                
                feature = make_json_serializable(feature)
                features.append(feature)
    
    return features

def extract_metric_features(data, message_ts):
    features = []
    resource_metrics = data.get("resourceMetrics", [])
    
    message_ts = normalize_datetime(message_ts)
    if message_ts is None:
        message_ts = datetime.now(timezone.utc)
    
    for rm in resource_metrics:
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
                
                for dp in metric_data_points:
                    timestamp_nano = dp.get("timeUnixNano")
                    timestamp = message_ts
                    if timestamp_nano:
                        try:
                            timestamp = datetime.fromtimestamp(int(timestamp_nano) / 1e9, tz=timezone.utc)
                        except (ValueError, TypeError, OverflowError):
                            timestamp = message_ts
                    
                    value = 0.0
                    if "asDouble" in dp:
                        value = dp["asDouble"]
                    elif "asInt" in dp:
                        value = float(dp["asInt"])
                    elif "count" in dp:
                        value = float(dp["count"])
                    
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
                    
                    feature = make_json_serializable(feature)
                    features.append(feature)
    
    return features

def extract_enhanced_features(messages, window_minutes=5):
    if not messages:
        return {}
    
    processed_messages = []
    for msg in messages:
        processed_msg = msg.copy()
        if 'timestamp' in processed_msg:
            processed_msg['timestamp'] = datetime_to_naive_utc(processed_msg['timestamp'])
        if 'observed_ts' in processed_msg:
            processed_msg['observed_ts'] = datetime_to_naive_utc(processed_msg['observed_ts'])
        processed_messages.append(processed_msg)
    
    df = pd.DataFrame(processed_messages)
    
    rate_features = {
        'messages_per_minute': len(df) / max(window_minutes, 1),
        'unique_sources': df.get('source', pd.Series()).nunique() if 'source' in df.columns else 0,
        'window_minutes': window_minutes
    }
    
    if 'severity_number' in df.columns:
        rate_features['error_rate'] = df['severity_number'].ge(17).mean()
    elif 'is_error' in df.columns:
        rate_features['error_rate'] = df['is_error'].mean()
    else:
        rate_features['error_rate'] = 0.0
    
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
    
    if 'hour' in df.columns and not df['hour'].empty:
        rate_features['peak_hour'] = int(df['hour'].mode().iloc[0])
        rate_features['hour_variance'] = float(df['hour'].var()) if len(df['hour']) > 1 else 0.0
    
    return rate_features

def calculate_sliding_features(topic, current_batch_data, window_sizes=[5, 15, 60]):
    features = {}
    current_time = datetime_to_naive_utc(datetime.now(timezone.utc))
    
    historical_data = list(historical_cache[topic])
    
    if not historical_data:
        return features
    
    processed_data = []
    for item in historical_data:
        processed_item = item.copy()
        if 'timestamp' in processed_item:
            processed_item['timestamp'] = datetime_to_naive_utc(processed_item['timestamp'])
        processed_data.append(processed_item)
    
    df = pd.DataFrame(processed_data)
    if 'timestamp' not in df.columns or df['timestamp'].isna().all():
        return features
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    for window in window_sizes:
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
        
        if 'severity_number' in window_data.columns:
            features[f'error_rate_{window}m'] = (window_data['severity_number'] >= 17).mean()
        elif 'is_error' in window_data.columns:
            features[f'error_rate_{window}m'] = window_data['is_error'].mean()
        else:
            features[f'error_rate_{window}m'] = 0.0
        
        if 'value' in window_data.columns and len(window_data) > 1:
            values = pd.to_numeric(window_data['value'], errors='coerce').dropna().values
            features[f'trend_{window}m'] = calculate_trend(values)
        else:
            features[f'trend_{window}m'] = 0.0
        
        if len(window_data) > 1:
            window_data_sorted = window_data.sort_values('timestamp')
            time_buckets = pd.cut(window_data_sorted['timestamp'], bins=5, labels=False)
            bucket_counts = window_data_sorted.groupby(time_buckets).size().values
            features[f'volume_trend_{window}m'] = calculate_trend(bucket_counts)
            
            if 'duration_ms' in window_data.columns:
                duration_data = pd.to_numeric(window_data['duration_ms'], errors='coerce').dropna().values
                if len(duration_data) > 0:
                    features[f'duration_trend_{window}m'] = calculate_trend(duration_data)
                    features[f'avg_duration_{window}m'] = float(np.mean(duration_data))
    
    return features

def extract_infra_health_features(metrics_data):
    health_features = {}
    
    if not metrics_data:
        return health_features
    
    df = pd.DataFrame(metrics_data)
    
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
    correlations = {}
    
    log_errors = sum(1 for log in logs if log.get('error') or log.get('severity_number', 0) >= 17)
    trace_errors = sum(1 for trace in traces if trace.get('is_error') or trace.get('exception'))
    metric_anomalies = sum(1 for metric in metrics if metric.get('value', 0) > 1000)
    
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
    
    trace_services = set()
    for trace in traces:
        attrs = trace.get('attributes', {})
        service_name = attrs.get('service.name')
        if service_name:
            trace_services.add(service_name)
    
    correlations['service_diversity'] = len(trace_services)
    
    if logs and traces:
        log_times = [log.get('observed_ts') for log in logs if log.get('observed_ts')]
        trace_times = [trace.get('start_time') for trace in traces if trace.get('start_time')]
        
        if log_times and trace_times:
            correlations['temporal_overlap'] = 1 if len(set(log_times) & set(trace_times)) > 0 else 0
    
    return correlations