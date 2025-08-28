import json
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from utils import normalize_datetime, datetime_to_naive_utc, calculate_trend, make_json_serializable, get_attr_value
from cache import get_recent_messages, historical_cache

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
