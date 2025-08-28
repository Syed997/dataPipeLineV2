from datetime import datetime, timezone
import numpy as np

def get_attr_value(attr_dict, expected_type="stringValue"):
    if not isinstance(attr_dict, dict):
        return None
    return attr_dict.get(expected_type)

def calculate_trend(values):
    if len(values) < 2:
        return 0.0
    
    values = np.array(values, dtype=float)
    if np.all(np.isnan(values)) or len(values) == 0:
        return 0.0
    
    valid_values = values[~np.isnan(values)]
    if len(valid_values) < 2:
        return 0.0
    
    x = np.arange(len(valid_values))
    try:
        slope = np.polyfit(x, valid_values, 1)[0]
        return float(slope)
    except (np.linalg.LinAlgError, ValueError):
        return 0.0

def normalize_datetime(dt):
    if dt is None:
        return None
    
    if isinstance(dt, str):
        try:
            if dt.endswith('Z'):
                dt = dt[:-1] + '+00:00'
            dt = datetime.fromisoformat(dt)
        except ValueError:
            try:
                dt = datetime.fromisoformat(dt)
            except ValueError:
                return None
    
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        elif dt.tzinfo != timezone.utc:
            dt = dt.astimezone(timezone.utc)
    
    return dt

def datetime_to_naive_utc(dt):
    dt = normalize_datetime(dt)
    if dt is None:
        return None
    return dt.replace(tzinfo=None)

def make_json_serializable(obj):
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