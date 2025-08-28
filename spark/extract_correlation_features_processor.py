import json
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from utils import normalize_datetime, datetime_to_naive_utc, calculate_trend, make_json_serializable, get_attr_value
from cache import get_recent_messages, historical_cache

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
