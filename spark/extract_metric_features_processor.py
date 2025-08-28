import json
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from utils import normalize_datetime, datetime_to_naive_utc, calculate_trend, make_json_serializable, get_attr_value
from cache import get_recent_messages, historical_cache

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
