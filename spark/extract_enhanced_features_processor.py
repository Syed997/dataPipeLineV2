import json
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from utils import normalize_datetime, datetime_to_naive_utc, calculate_trend, make_json_serializable, get_attr_value
from cache import get_recent_messages, historical_cache

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
