from datetime import datetime
from collections import defaultdict
import json
from extract_log_features_processor import extract_log_features
from extract_trace_features_processor import extract_trace_features
from extract_metric_features_processor import extract_metric_features
from extract_enhanced_features_processor import extract_enhanced_features
from calculate_sliding_features_processor import calculate_sliding_features
from extract_infra_health_features_processor import extract_infra_health_features
from extract_correlation_features_processor import extract_correlation_features
from cache import update_historical_cache, get_recent_messages
from utils import calculate_trend

def save_to_clickhouse_enhanced(batch_df, batch_id: int, client):
    print(f"Processing batch {batch_id} at {datetime.utcnow()}")
    
    rows = batch_df.collect()
    if not rows:
        print(f"Batch {batch_id}: No data to process")
        return
        
    timestamps = [r["ts"] for r in rows]
    window_start = min(timestamps)
    window_end = max(timestamps)
    
    print(f"Batch {batch_id}: Processing {len(rows)} messages")
    print(f"Batch {batch_id}: Time window {window_start} to {window_end}")
    
    insert_data = []
    aggregated_features = []
    
    topic_data = defaultdict(list)
    for r in rows:
        topic_data[r["topic"]].append(r)
    
    for topic, topic_rows in topic_data.items():
        print(f"Batch {batch_id}: Processing {len(topic_rows)} messages for topic {topic}")
        
        all_logs = []
        all_traces = []
        all_metrics = []
        
        for r in topic_rows:
            value = r["value"]
            ts = r["ts"]
            features_json = ""
            enhanced_features_json = ""
            sliding_features_json = ""
            correlation_features_json = ""
            
            try:
                data = json.loads(value)
                extracted_features = {}
                
                if "resourceLogs" in data:
                    log_features = extract_log_features(data, ts)
                    if log_features:
                        extracted_features["logs"] = log_features
                        all_logs.extend(log_features)
                        for log_feature in log_features:
                            update_historical_cache(topic, log_feature)
                
                if "resourceSpans" in data:
                    trace_features = extract_trace_features(data, ts)
                    if trace_features:
                        extracted_features["traces"] = trace_features
                        all_traces.extend(trace_features)
                        for trace_feature in trace_features:
                            update_historical_cache(topic, trace_feature)
                
                if "resourceMetrics" in data:
                    metric_features = extract_metric_features(data, ts)
                    if metric_features:
                        extracted_features["metrics"] = metric_features
                        all_metrics.extend(metric_features)
                        for metric_feature in metric_features:
                            update_historical_cache(topic, metric_feature)
                
                if extracted_features:
                    features_json = json.dumps(extracted_features, separators=(',', ':'))
                    
            except json.JSONDecodeError as e:
                print(f"Batch {batch_id}: Error parsing JSON for {topic}: {e}")
                continue
            except Exception as e:
                print(f"Batch {batch_id}: Unexpected error during feature extraction for {topic}: {e}")
                continue
            
            try:
                recent_messages = get_recent_messages(topic, window_minutes=5)
                if recent_messages:
                    enhanced_features = extract_enhanced_features(recent_messages, window_minutes=5)
                    if enhanced_features:
                        enhanced_features_json = json.dumps(enhanced_features, separators=(',', ':'))
            except Exception as e:
                print(f"Batch {batch_id}: Error extracting enhanced features for {topic}: {e}")
            
            try:
                sliding_features = calculate_sliding_features(topic, batch_df, window_sizes=[5, 15, 60])
                if sliding_features:
                    sliding_features_json = json.dumps(sliding_features, separators=(',', ':'))
            except Exception as e:
                print(f"Batch {batch_id}: Error extracting sliding features for {topic}: {e}")
            
            insert_data.append((
                topic,
                value,
                ts,
                batch_id,
                window_start,
                window_end,
                features_json,
                enhanced_features_json,
                sliding_features_json,
                ""
            ))
        
        try:
            if all_logs or all_traces or all_metrics:
                correlation_features = extract_correlation_features(all_logs, all_metrics, all_traces)
                
                if all_metrics:
                    infra_features = extract_infra_health_features(all_metrics)
                    correlation_features.update(infra_features)
                
                correlation_features_json = json.dumps(correlation_features, separators=(',', ':'))
                
                for i, (topic_name, value, ts, batch_id_val, ws, we, feat, enh_feat, slide_feat, _) in enumerate(insert_data):
                    if topic_name == topic:
                        insert_data[i] = (topic_name, value, ts, batch_id_val, ws, we, feat, enh_feat, slide_feat, correlation_features_json)
        
        except Exception as e:
            print(f"Batch {batch_id}: Error extracting correlation features for {topic}: {e}")
        
        try:
            recent_messages = get_recent_messages(topic, window_minutes=5)
            if recent_messages:
                enhanced_features = extract_enhanced_features(recent_messages, window_minutes=5)
                
                values = [msg.get('value', 0) for msg in recent_messages]
                trend_coeff = calculate_trend(values) if len(values) > 1 else 0.0
                
                aggregated_feature = (
                    topic,
                    window_start,
                    window_end,
                    5,
                    enhanced_features.get('messages_per_minute', 0.0),
                    enhanced_features.get('error_rate', 0.0),
                    enhanced_features.get('unique_sources', 0),
                    enhanced_features.get('body_length_mean', 0.0),
                    enhanced_features.get('duration_ms_mean', 0.0),
                    enhanced_features.get('duration_ms_p95', 0.0),
                    trend_coeff
                )
                aggregated_features.append(aggregated_feature)
        except Exception as e:
            print(f"Batch {batch_id}: Error creating aggregated features for {topic}: {e}")
    
    try:
        if insert_data:
            client.execute(
                "INSERT INTO messages (topic, value, ts, batch_id, window_start, window_end, features, enhanced_features, sliding_features, correlation_features) VALUES",
                insert_data
            )
            print(f"Batch {batch_id}: Successfully inserted {len(insert_data)} message records")
        
        if aggregated_features:
            client.execute(
                "INSERT INTO aggregated_features (topic, window_start, window_end, window_minutes, messages_per_minute, error_rate, unique_sources, avg_body_length, avg_duration_ms, p95_duration_ms, trend_coefficient) VALUES",
                aggregated_features
            )
            print(f"Batch {batch_id}: Successfully inserted {len(aggregated_features)} aggregated feature records")
        
        feature_counts = {
            "total": len(insert_data),
            "with_features": sum(1 for item in insert_data if item[6]),
            "with_enhanced": sum(1 for item in insert_data if item[7]),
            "with_sliding": sum(1 for item in insert_data if item[8]),
            "with_correlation": sum(1 for item in insert_data if item[9]),
            "logs": sum(1 for item in insert_data if '"logs":' in item[6]),
            "traces": sum(1 for item in insert_data if '"traces":' in item[6]),
            "metrics": sum(1 for item in insert_data if '"metrics":' in item[6])
        }
        
        print(f"Batch {batch_id}: Feature extraction summary:")
        print(f"  - Total messages: {feature_counts['total']}")
        print(f"  - With original features: {feature_counts['with_features']}")
        print(f"  - With enhanced features: {feature_counts['with_enhanced']}")
        print(f"  - With sliding features: {feature_counts['with_sliding']}")
        print(f"  - With correlation features: {feature_counts['with_correlation']}")
        print(f"  - Logs: {feature_counts['logs']}, Traces: {feature_counts['traces']}, Metrics: {feature_counts['metrics']}")
        
        topic_counts = {}
        for item in insert_data:
            topic_counts[item[0]] = topic_counts.get(item[0], 0) + 1
        print(f"Batch {batch_id}: Topic distribution: {topic_counts}")
        
    except Exception as e:
        print(f"Batch {batch_id}: Error inserting data: {e}")
        raise