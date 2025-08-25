import os
import threading
import time
from datetime import datetime
from flask import Flask, request, jsonify
from pyspark.sql import SparkSession, functions as F, types as T
from clickhouse_driver import Client

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PATTERN = os.environ.get("TOPIC_PATTERN", "demo-.*")
API_PORT = int(os.environ.get("API_PORT", "5001"))

# ---------- ClickHouse Connection ----------
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "kafka_logs")

client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)

# Enhanced table with batch metadata
client.execute(f"""
CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.messages (
    topic String,
    value String,
    ts DateTime,
    batch_id UInt64,
    window_start DateTime,
    window_end DateTime,
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ts, topic)
""")

# ---------- Flask API ----------
app = Flask(__name__)

@app.get("/health")
def health():
    return {"status": "ok", "server_time_utc": datetime.utcnow().isoformat() + "Z"}

@app.get("/topics")
def list_topics():
    """Get all available topics with message counts"""
    try:
        result = client.execute("""
            SELECT 
                topic,
                count(*) as total_messages,
                min(ts) as first_message,
                max(ts) as last_message,
                count(DISTINCT batch_id) as batch_count
            FROM messages 
            GROUP BY topic
            ORDER BY total_messages DESC
        """)
        
        topics = []
        for row in result:
            topics.append({
                "topic": row[0],
                "total_messages": row[1],
                "first_message": row[2].isoformat() + "Z" if row[2] else None,
                "last_message": row[3].isoformat() + "Z" if row[3] else None,
                "batch_count": row[4]
            })
        
        return {"topics": topics}
        
    except Exception as e:
        return {"error": str(e)}, 500

@app.get("/count")
def count_messages():
    """Count messages for a specific topic in the last 5 seconds"""
    topic = request.args.get('topic')
    if not topic:
        return {"error": "Topic parameter is required"}, 400
    
    try:
        # Count messages for the specific topic in the last 5 seconds
        # Using %(topic)s syntax for ClickHouse parameter binding
        result = client.execute("""
            SELECT count(*) as message_count
            FROM messages 
            WHERE topic = %(topic)s 
            AND processed_at >= now() - INTERVAL 5 SECOND
        """, {'topic': topic})
        
        count = result[0][0] if result else 0
        
        # Also get some additional stats
        stats_result = client.execute("""
            SELECT 
                count(*) as total_count,
                min(ts) as earliest_message,
                max(ts) as latest_message,
                min(processed_at) as first_processed,
                max(processed_at) as last_processed
            FROM messages 
            WHERE topic = %(topic)s 
            AND processed_at >= now() - INTERVAL 5 SECOND
        """, {'topic': topic})
        
        if stats_result and stats_result[0][0] > 0:
            stats = stats_result[0]
            return {
                "topic": topic,
                "message_count": count,
                "time_window": "last 5 seconds",
                "earliest_message": stats[1].isoformat() + "Z" if stats[1] else None,
                "latest_message": stats[2].isoformat() + "Z" if stats[2] else None,
                "first_processed": stats[3].isoformat() + "Z" if stats[3] else None,
                "last_processed": stats[4].isoformat() + "Z" if stats[4] else None
            }
        else:
            return {
                "topic": topic,
                "message_count": 0,
                "time_window": "last 5 seconds",
                "earliest_message": None,
                "latest_message": None,
                "first_processed": None,
                "last_processed": None
            }
            
    except Exception as e:
        return {"error": str(e)}, 500

@app.get("/batches/recent")
def recent_batches():
    """Get recent batch statistics"""
    result = client.execute("""
        SELECT 
            batch_id,
            window_start,
            window_end,
            count(*) as message_count,
            countDistinct(topic) as topic_count,
            min(ts) as earliest_message,
            max(ts) as latest_message
        FROM messages 
        WHERE processed_at >= now() - INTERVAL 1 HOUR
        GROUP BY batch_id, window_start, window_end
        ORDER BY batch_id DESC
        LIMIT 10
    """)
    
    batches = []
    for row in result:
        batches.append({
            "batch_id": row[0],
            "window_start": row[1].isoformat() + "Z",
            "window_end": row[2].isoformat() + "Z",
            "message_count": row[3],
            "topic_count": row[4],
            "earliest_message": row[5].isoformat() + "Z" if row[5] else None,
            "latest_message": row[6].isoformat() + "Z" if row[6] else None
        })
    
    return {"batches": batches}

def run_api():
    app.run(host="0.0.0.0", port=API_PORT)

# ---------- Spark Structured Streaming job with Windowing ----------
def main():
    # Start API in background
    threading.Thread(target=run_api, daemon=True).start()
    
    spark = (SparkSession.builder
             .appName("KafkaToClickHouse")
             .config("spark.sql.streaming.metricsEnabled", "true")
             .getOrCreate())
    
    # Read Kafka stream
    df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "latest")
        .load())
    
    # Extract and transform data
    events = df.select(
        F.col("topic").cast("string").alias("topic"),
        F.col("value").cast("string").alias("value"),
        F.col("timestamp").alias("ts")
    )
    
    # METHOD 1: Simple approach - process every 5 seconds (your current approach)
    # This naturally gets the last 5 seconds of data due to the trigger interval
    
    # METHOD 2: Explicit windowing approach (optional for more control)
    # Uncomment this section if you want explicit 5-second tumbling windows
    """
    windowed_events = events \
        .withWatermark("ts", "10 seconds") \
        .groupBy(
            F.window(F.col("ts"), "5 seconds"),
            F.col("topic")
        ) \
        .agg(
            F.collect_list("value").alias("values"),
            F.count("*").alias("count"),
            F.min("ts").alias("min_ts"),
            F.max("ts").alias("max_ts")
        ) \
        .select(
            F.col("topic"),
            F.explode("values").alias("value"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("min_ts").alias("ts")
        )
    """
    
    # Custom sink function with batch metadata
    def save_to_clickhouse(batch_df, batch_id: int):
        print(f"Processing batch {batch_id} at {datetime.utcnow()}")
        
        # Collect all rows
        rows = batch_df.collect()
        if not rows:
            print(f"Batch {batch_id}: No data to process")
            return
            
        # Calculate window boundaries for this batch
        timestamps = [r["ts"] for r in rows]
        window_start = min(timestamps)
        window_end = max(timestamps)
        
        print(f"Batch {batch_id}: Processing {len(rows)} messages")
        print(f"Batch {batch_id}: Time window {window_start} to {window_end}")
        
        # Prepare data for insertion with batch metadata
        insert_data = [
            (
                r["topic"],
                r["value"], 
                r["ts"],  # Already a datetime object, no conversion needed
                batch_id,
                window_start,  # Already a datetime object
                window_end     # Already a datetime object
            )
            for r in rows
        ]
        
        try:
            # Bulk insert to ClickHouse
            client.execute(
                "INSERT INTO messages (topic, value, ts, batch_id, window_start, window_end) VALUES",
                insert_data
            )
            print(f"Batch {batch_id}: Successfully inserted {len(insert_data)} records")
            
            # Log batch statistics
            topic_counts = {}
            for r in rows:
                topic_counts[r["topic"]] = topic_counts.get(r["topic"], 0) + 1
            
            print(f"Batch {batch_id}: Topic distribution: {topic_counts}")
            
        except Exception as e:
            print(f"Batch {batch_id}: Error inserting data: {e}")
            raise
    
    # Start the streaming query
    query = (events.writeStream
        .outputMode("append")
        .foreachBatch(save_to_clickhouse)
        .option("checkpointLocation", "/tmp/spark-clickhouse-checkpoints")
        .trigger(processingTime="5 seconds")  # Process every 5 seconds
        .start())
    
    print("🚀 Streaming job started!")
    print(f"📊 Processing data every 5 seconds from topics: {TOPIC_PATTERN}")
    print(f"💾 Saving to ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}")
    print(f"🔍 API endpoints available:")
    print(f"   - Health: http://localhost:{API_PORT}/health")
    print(f"   - Count: http://localhost:{API_PORT}/count?topic=TOPIC_NAME")
    print(f"   - Topics: http://localhost:{API_PORT}/topics")
    print(f"   - Batches: http://localhost:{API_PORT}/batches/recent")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏹️  Stopping streaming job...")
        query.stop()

if __name__ == "__main__":
    main()