import threading
from pyspark.sql import SparkSession, functions as F
from config import get_config
from clickhouse_setup import initialize_clickhouse
from api import create_app
from spark_streaming import save_to_clickhouse_enhanced
from cache import clear_cache

def main():
    config = get_config()
    client = initialize_clickhouse(config)
    app = create_app(client)
    
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=config['API_PORT']), daemon=True).start()
    
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