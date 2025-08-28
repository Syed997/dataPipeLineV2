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
