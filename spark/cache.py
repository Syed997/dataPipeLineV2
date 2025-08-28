from collections import defaultdict, deque
from utils import datetime_to_naive_utc

# Global cache initialization
historical_cache = defaultdict(lambda: deque(maxlen=10000))
feature_cache = defaultdict(dict)

def update_historical_cache(topic, message_data):
    cache_entry = {}
    
    if 'timestamp' in message_data:
        cache_entry['timestamp'] = datetime_to_naive_utc(message_data['timestamp'])
    elif 'observed_ts' in message_data:
        cache_entry['timestamp'] = datetime_to_naive_utc(message_data['observed_ts'])
    else:
        cache_entry['timestamp'] = datetime_to_naive_utc(datetime.now(timezone.utc))
    
    safe_fields = ['severity_number', 'is_error', 'value', 'duration_ms', 'body_length', 'source', 'hour']
    for field in safe_fields:
        if field in message_data:
            value = message_data[field]
            if isinstance(value, (int, float, str, bool, type(None))):
                cache_entry[field] = value
            else:
                cache_entry[field] = str(value)
    
    cache_entry.setdefault('severity_number', 0)
    cache_entry.setdefault('is_error', 0)
    cache_entry.setdefault('value', 0)
    cache_entry.setdefault('duration_ms', 0)
    cache_entry.setdefault('body_length', 0)
    cache_entry.setdefault('source', '')
    cache_entry.setdefault('hour', cache_entry['timestamp'].hour if cache_entry['timestamp'] else 0)
    
    historical_cache[topic].append(cache_entry)

def get_recent_messages(topic, window_minutes=5):
    from datetime import datetime, timedelta, timezone
    cutoff_time = datetime_to_naive_utc(datetime.now(timezone.utc)) - timedelta(minutes=window_minutes)
    historical_data = list(historical_cache[topic])
    
    recent_messages = []
    for msg in historical_data:
        msg_time = msg.get('timestamp')
        if msg_time is None:
            continue
            
        msg_time = datetime_to_naive_utc(msg_time)
        if msg_time is None:
            continue
        
        if msg_time >= cutoff_time:
            recent_messages.append(msg)
    
    return recent_messages

def clear_cache():
    historical_cache.clear()
    feature_cache.clear()