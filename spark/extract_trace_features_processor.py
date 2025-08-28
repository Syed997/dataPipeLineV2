import json
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from utils import normalize_datetime, datetime_to_naive_utc, calculate_trend, make_json_serializable, get_attr_value
from cache import get_recent_messages, historical_cache

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
