import threading
import time
import logging
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from config import get_config
from clickhouse_setup import initialize_clickhouse
from api import create_app
from spark_streaming import save_to_clickhouse_enhanced
from cache import clear_cache
from clickhouse_csv_exporter import export_kafka_logs_to_csv

def csv_export_worker(client, config):
    """Worker function to export CSV every 2 minutes"""
    logger = logging.getLogger(__name__)
    
    while True:
        try:
            # Create timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = config.get('CSV_OUTPUT_DIR', './exports')
            
            # Create output directory if it doesn't exist
            import os
            os.makedirs(output_dir, exist_ok=True)
            
            output_file = f"{output_dir}/kafka_logs_export_{timestamp}.csv"
            
            # Export to CSV with optional limit
            success = export_kafka_logs_to_csv(
                client, 
                output_file=output_file,
                limit=config.get('CSV_EXPORT_LIMIT', 1000000),  # Default limit of 1000 records
                exclude_columns=['context_response_data']
            )
            print("******")
            print(success)
            if success:
                logger.info(f"✅ CSV export completed: {output_file}")
                print(f"📄 CSV exported: {output_file}")
                
                # Optional: Clean up old files (keep last 10 files)
                cleanup_old_csv_files(output_dir, keep_count=2)
            else:
                logger.error("❌ CSV export failed")
                print("❌ CSV export failed")
                
        except Exception as e:
            logger.error(f"Error in CSV export worker: {e}")
            print(f"❌ CSV export error: {e}")
        
        # Wait 2 minutes (120 seconds)
        time.sleep(120)

def cleanup_old_csv_files(output_dir, keep_count=10):
    """Clean up old CSV files, keeping only the most recent ones"""
    try:
        import os
        import glob
        
        # Get all CSV files with the pattern
        csv_files = glob.glob(os.path.join(output_dir, "kafka_logs_export_*.csv"))
        
        # Sort by modification time (newest first)
        csv_files.sort(key=os.path.getmtime, reverse=True)
        
        # Remove older files
        if len(csv_files) > keep_count:
            for old_file in csv_files[keep_count:]:
                os.remove(old_file)
                print(f"🗑️  Cleaned up old CSV: {os.path.basename(old_file)}")
                
    except Exception as e:
        logging.getLogger(__name__).warning(f"Could not cleanup old CSV files: {e}")

def main():
    config = get_config()
    client = initialize_clickhouse(config)
    app = create_app(client)
   
    # Start API server
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=config['API_PORT']), daemon=True).start()
   
    # Start CSV export worker (every 2 minutes)
    if config.get('ENABLE_CSV_EXPORT', True):
        csv_thread = threading.Thread(target=csv_export_worker, args=(client, config), daemon=True)
        csv_thread.start()
        print("📄 CSV export worker started (exports every 2 minutes)")
   
    spark = (SparkSession.builder
             .appName("EnhancedKafkaToClickHouse")
             .config("spark.sql.streaming.metricsEnabled", "true")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
   
    spark.sparkContext.setLogLevel("WARN")
   
    df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config['BOOTSTRAP'])
        .option("subscribePattern", config['TOPIC_PATTERN'])
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())
   
    events = df.select(
        F.col("topic").cast("string").alias("topic"),
        F.col("value").cast("string").alias("value"),
        F.col("timestamp").alias("ts")
    )
   
    query = (events.writeStream
        .outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: save_to_clickhouse_enhanced(batch_df, batch_id, client))
        .option("checkpointLocation", "/tmp/spark-clickhouse-enhanced-checkpoints")
        .trigger(processingTime="5 seconds")
        .start())
   
    print("🚀 Enhanced Streaming job started!")
    print(f"📊 Processing data every 5 seconds from topics: {config['TOPIC_PATTERN']}")
    print(f"💾 Saving to ClickHouse: {config['CLICKHOUSE_HOST']}:{config['CLICKHOUSE_PORT']}/{config['CLICKHOUSE_DB']}")
    print(f"📄 CSV exports every 2 minutes to: {config.get('CSV_OUTPUT_DIR', './exports')}")
    print(f"🔍 Enhanced API endpoints available:")
    print(f"   - Health: http://localhost:{config['API_PORT']}/health")
    print(f"   - Topics: http://localhost:{config['API_PORT']}/topics")
    print(f"   - Enhanced Features: http://localhost:{config['API_PORT']}/features/enhanced")
    print(f"   - Trends: http://localhost:{config['API_PORT']}/trends/<topic>")
    print(f"   - Count: http://localhost:{config['API_PORT']}/count?topic=TOPIC_NAME")
    print(f"   - Batches: http://localhost:{config['API_PORT']}/batches/recent")
    print(f"   - Feature Stats: http://localhost:{config['API_PORT']}/features/stats")
    print(f"   - Feature Samples: http://localhost:{config['API_PORT']}/features/sample?topic=TOPIC_NAME&limit=5")
    print(f"📈 New features include:")
    print(f"   - Time-based aggregations (messages/min, error rates)")
    print(f"   - Sliding window analysis (5m, 15m, 1h)")
    print(f"   - Infrastructure health metrics")
    print(f"   - Cross-signal correlations")
    print(f"   - Trend analysis and anomaly detection")
   
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping enhanced streaming job...")
        query.stop()
    finally:
        clear_cache()
        print("🧹 Cache cleaned up")

if __name__ == "__main__":
    main()












# import threading
# from pyspark.sql import SparkSession, functions as F
# from config import get_config
# from clickhouse_setup import initialize_clickhouse
# from api import create_app
# from spark_streaming import save_to_clickhouse_enhanced
# from cache import clear_cache

# def main():
#     config = get_config()
#     client = initialize_clickhouse(config)
#     app = create_app(client)
    
#     threading.Thread(target=lambda: app.run(host="0.0.0.0", port=config['API_PORT']), daemon=True).start()
    
#     spark = (SparkSession.builder
#              .appName("EnhancedKafkaToClickHouse")
#              .config("spark.sql.streaming.metricsEnabled", "true")
#              .config("spark.sql.adaptive.enabled", "true")
#              .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
#              .getOrCreate())
    
#     spark.sparkContext.setLogLevel("WARN")
    
#     df = (spark.readStream
#         .format("kafka")
#         .option("kafka.bootstrap.servers", config['BOOTSTRAP'])
#         .option("subscribePattern", config['TOPIC_PATTERN'])
#         .option("startingOffsets", "latest")
#         .option("failOnDataLoss", "false")
#         .load())
    
#     events = df.select(
#         F.col("topic").cast("string").alias("topic"),
#         F.col("value").cast("string").alias("value"),
#         F.col("timestamp").alias("ts")
#     )
    
#     query = (events.writeStream
#         .outputMode("append")
#         .foreachBatch(lambda batch_df, batch_id: save_to_clickhouse_enhanced(batch_df, batch_id, client))
#         .option("checkpointLocation", "/tmp/spark-clickhouse-enhanced-checkpoints")
#         .trigger(processingTime="5 seconds")
#         .start())
    
#     print("🚀 Enhanced Streaming job started!")
#     print(f"📊 Processing data every 5 seconds from topics: {config['TOPIC_PATTERN']}")
#     print(f"💾 Saving to ClickHouse: {config['CLICKHOUSE_HOST']}:{config['CLICKHOUSE_PORT']}/{config['CLICKHOUSE_DB']}")
#     print(f"🔍 Enhanced API endpoints available:")
#     print(f"   - Health: http://localhost:{config['API_PORT']}/health")
#     print(f"   - Topics: http://localhost:{config['API_PORT']}/topics")
#     print(f"   - Enhanced Features: http://localhost:{config['API_PORT']}/features/enhanced")
#     print(f"   - Trends: http://localhost:{config['API_PORT']}/trends/<topic>")
#     print(f"   - Count: http://localhost:{config['API_PORT']}/count?topic=TOPIC_NAME")
#     print(f"   - Batches: http://localhost:{config['API_PORT']}/batches/recent")
#     print(f"   - Feature Stats: http://localhost:{config['API_PORT']}/features/stats")
#     print(f"   - Feature Samples: http://localhost:{config['API_PORT']}/features/sample?topic=TOPIC_NAME&limit=5")
#     print(f"📈 New features include:")
#     print(f"   - Time-based aggregations (messages/min, error rates)")
#     print(f"   - Sliding window analysis (5m, 15m, 1h)")
#     print(f"   - Infrastructure health metrics")
#     print(f"   - Cross-signal correlations")
#     print(f"   - Trend analysis and anomaly detection")
    
#     try:
#         query.awaitTermination()
#     except KeyboardInterrupt:
#         print("\n⏹️  Stopping enhanced streaming job...")
#         query.stop()
#     finally:
#         clear_cache()
#         print("🧹 Cache cleaned up")

# if __name__ == "__main__":
#     main()