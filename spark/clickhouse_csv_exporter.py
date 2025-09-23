import pandas as pd
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional

# Set up basic logging
logging.basicConfig(level=logging.INFO)

class ClickHouseCSVExporter:
    """Export ClickHouse kafka_logs data to CSV format with flattened JSON features"""
    
    def __init__(self, clickhouse_client):
        self.client = clickhouse_client
        self.logger = logging.getLogger(__name__)
        
    def extract_timestamp_from_log(self, log_entry: Dict[str, Any]) -> Optional[str]:
        """Extract timestamp from a single log entry"""
        try:
            for ts_field in ['timestamp', 'observed_ts', 'time', 'ts']:
                if ts_field in log_entry:
                    return log_entry[ts_field]
        except (KeyError, TypeError) as e:
            self.logger.warning(f"Could not extract timestamp from log entry: {e}")
        return None
    
    def extract_timestamp_from_features(self, features_data: Dict[str, Any]) -> Optional[str]:
        """Extract timestamp from features data - handles different structures"""
        try:
            # Try to get timestamp from logs array (first structure)
            if 'logs' in features_data and isinstance(features_data['logs'], list) and features_data['logs']:
                return self.extract_timestamp_from_log(features_data['logs'][0])
            
            # Try to get timestamp from metrics array (second structure)
            if 'metrics' in features_data and isinstance(features_data['metrics'], list) and features_data['metrics']:
                metric = features_data['metrics'][0]
                if isinstance(metric, dict) and 'timestamp' in metric:
                    return metric['timestamp']
            
            # Try to get timestamp from traces array
            if 'traces' in features_data and isinstance(features_data['traces'], list) and features_data['traces']:
                trace = features_data['traces'][0]
                if isinstance(trace, dict) and 'timestamp' in trace:
                    return trace['timestamp']
                    
            # Try direct timestamp field
            if 'timestamp' in features_data:
                return features_data['timestamp']
                
        except (KeyError, TypeError) as e:
            self.logger.warning(f"Could not extract timestamp from features: {e}")
        return None
    
    def flatten_json(self, data: Any, prefix: str = "") -> Dict[str, Any]:
        """Flatten a JSON object into a dictionary with optional prefix"""
        flattened = {}
        try:
            if isinstance(data, str):
                data = json.loads(data)  # Parse string to JSON if needed
            if isinstance(data, dict):
                for key, value in data.items():
                    new_key = f"{prefix}_{key}" if prefix else key
                    if isinstance(value, dict):
                        flattened.update(self.flatten_json(value, new_key))
                    elif isinstance(value, list):
                        flattened[new_key] = json.dumps(value) if value else None
                    else:
                        flattened[new_key] = value
            else:
                flattened[prefix] = json.dumps(data) if data else None
            return flattened
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.warning(f"Could not flatten JSON: {e}")
            return {}
    
    def process_features_data(self, features_data: Dict[str, Any], topic: str, 
                            enhanced_flat: Dict[str, Any], sliding_flat: Dict[str, Any], 
                            correlation_flat: Dict[str, Any]) -> list:
        """Process features data and return list of records"""
        processed_data = []
        
        # Extract timestamp once for this features object
        features_timestamp = self.extract_timestamp_from_features(features_data)
        
        # Handle logs structure
        if 'logs' in features_data and isinstance(features_data['logs'], list):
            for log_entry in features_data['logs']:
                record = {'topic': topic}
                timestamp = self.extract_timestamp_from_log(log_entry) or features_timestamp
                record['timestamp'] = timestamp
                
                # Flatten the log entry
                log_flat = self.flatten_json(log_entry)
                record.update(log_flat)
                record.update(enhanced_flat)
                record.update(sliding_flat)
                record.update(correlation_flat)
                processed_data.append(record)
        
        # Handle metrics structure
        elif 'metrics' in features_data and isinstance(features_data['metrics'], list):
            for metric_entry in features_data['metrics']:
                record = {'topic': topic}
                timestamp = metric_entry.get('timestamp') if isinstance(metric_entry, dict) else features_timestamp
                record['timestamp'] = timestamp
                
                # Flatten the metric entry
                metric_flat = self.flatten_json(metric_entry)
                record.update(metric_flat)
                record.update(enhanced_flat)
                record.update(sliding_flat)
                record.update(correlation_flat)
                processed_data.append(record)
        
        # Handle traces structure
        elif 'traces' in features_data and isinstance(features_data['traces'], list):
            for trace_entry in features_data['traces']:
                record = {'topic': topic}
                timestamp = trace_entry.get('timestamp') if isinstance(trace_entry, dict) else features_timestamp
                record['timestamp'] = timestamp
                
                # Flatten the trace entry
                trace_flat = self.flatten_json(trace_entry)
                record.update(trace_flat)
                record.update(enhanced_flat)
                record.update(sliding_flat)
                record.update(correlation_flat)
                processed_data.append(record)
        
        # Handle other structures - create single record with flattened features
        else:
            record = {'topic': topic}
            record['timestamp'] = features_timestamp
            
            # Flatten the entire features object
            features_flat = self.flatten_json(features_data, prefix="features")
            record.update(features_flat)
            record.update(enhanced_flat)
            record.update(sliding_flat)
            record.update(correlation_flat)
            processed_data.append(record)
            
        return processed_data
    
    def export_to_csv(self, 
                     output_file: str = "kafka_logs_export.csv",
                     limit: Optional[int] = None,
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None,
                     exclude_columns: Optional[list] = None) -> bool:
        """
        Export ClickHouse data to CSV, creating one row per log entry in features.logs
        
        Args:
            output_file: Output CSV file path
            limit: Limit number of records (None for all)
            start_date: Start date filter (YYYY-MM-DD format)
            end_date: End date filter (YYYY-MM-DD format)
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Build the base query
            query = """
            SELECT 
                topic,
                features,
                enhanced_features,
                sliding_features,
                correlation_features
            FROM kafka_logs.messages
            """
            
            # Add date filters if provided - make more flexible for different data structures
            conditions = []
            if start_date or end_date:
                # Try multiple timestamp extraction methods for filtering
                timestamp_conditions = []
                if start_date:
                    timestamp_conditions.extend([
                        f"toDate(JSONExtractString(features, 'logs', 1, 'timestamp')) >= '{start_date}'",
                        f"toDate(JSONExtractString(features, 'metrics', 1, 'timestamp')) >= '{start_date}'",
                        f"toDate(JSONExtractString(features, 'timestamp')) >= '{start_date}'"
                    ])
                if end_date:
                    timestamp_conditions.extend([
                        f"toDate(JSONExtractString(features, 'logs', 1, 'timestamp')) <= '{end_date}'",
                        f"toDate(JSONExtractString(features, 'metrics', 1, 'timestamp')) <= '{end_date}'",
                        f"toDate(JSONExtractString(features, 'timestamp')) <= '{end_date}'"
                    ])
                
                if timestamp_conditions:
                    conditions.append(f"({' OR '.join(timestamp_conditions)})")
                
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            # Use flexible ordering - try different timestamp paths
            query += """ ORDER BY 
                COALESCE(
                    JSONExtractString(features, 'logs', 1, 'timestamp'),
                    JSONExtractString(features, 'metrics', 1, 'timestamp'), 
                    JSONExtractString(features, 'timestamp'),
                    JSONExtractString(features, 'traces', 1, 'timestamp')
                ) DESC"""
                
            if limit:
                query += f" LIMIT {limit}"
                
            self.logger.info(f"Executing query: {query}")
            
            # Execute query and get results
            result = self.client.execute(query)
            
            if not result:
                self.logger.warning("No data found in the query result")
                return False
            
            # Process results into a list of dictionaries
            processed_data = []
            
            for row in result:
                topic, features, enhanced_features, sliding_features, correlation_features = row
                
                # Parse features JSON
                try:
                    features_data = json.loads(features) if isinstance(features, str) else features
                except (json.JSONDecodeError, TypeError) as e:
                    self.logger.warning(f"Could not parse features JSON: {e}")
                    continue
                
                # Flatten enhanced, sliding, and correlation features once per row
                enhanced_flat = self.flatten_json(enhanced_features, prefix="enhanced")
                sliding_flat = self.flatten_json(sliding_features, prefix="sliding")
                correlation_flat = self.flatten_json(correlation_features, prefix="correlation")
                
                # Process the features data based on its structure
                records = self.process_features_data(features_data, topic, enhanced_flat, sliding_flat, correlation_flat)
                processed_data.extend(records)
            
            # Set default exclusions if not provided
            if exclude_columns is None:
                exclude_columns = ['context_response_data']
            
            # Create DataFrame and export to CSV
            if not processed_data:
                self.logger.warning("No processed data to export")
                return False
                
            df = pd.DataFrame(processed_data)
            
            # Remove excluded columns
            columns_to_drop = [col for col in exclude_columns if col in df.columns]
            if columns_to_drop:
                df = df.drop(columns=columns_to_drop)
                self.logger.info(f"Excluded columns: {columns_to_drop}")
            
            # Reorder columns to put basic info first - only if they exist
            basic_cols = []
            if 'timestamp' in df.columns:
                basic_cols.append('timestamp')
            if 'topic' in df.columns:
                basic_cols.append('topic')
            
            remaining_cols = [col for col in df.columns if col not in basic_cols]
            cols = basic_cols + sorted(remaining_cols)
            df = df[cols]
            
            # Export to CSV
            df.to_csv(output_file, index=False)
            
            self.logger.info(f"Successfully exported {len(df)} records to {output_file}")
            self.logger.info(f"CSV contains {len(df.columns)} columns")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting to CSV: {e}")
            return False

def export_kafka_logs_to_csv(clickhouse_client, 
                           output_file: str = "kafka_logs_export.csv",
                           limit: Optional[int] = None,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           exclude_columns: Optional[list] = None) -> bool:
    """
    Convenience function to export ClickHouse kafka logs to CSV
    
    Args:
        clickhouse_client: ClickHouse client instance
        output_file: Output CSV file path
        limit: Limit number of records (None for all)
        start_date: Start date filter (YYYY-MM-DD format)
        end_date: End date filter (YYYY-MM-DD format)
        exclude_columns: List of column names to exclude from export
        
    Returns:
        bool: True if successful, False otherwise
    """
    exporter = ClickHouseCSVExporter(clickhouse_client)
    return exporter.export_to_csv(output_file, limit, start_date, end_date, exclude_columns)

