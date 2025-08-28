import json
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from utils import normalize_datetime, datetime_to_naive_utc, calculate_trend, make_json_serializable, get_attr_value
from cache import get_recent_messages, historical_cache

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
