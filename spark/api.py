from flask import Flask, jsonify
from datetime import datetime

def create_app(client):
    app = Flask(__name__)

    @app.get("/health")
    def health():
        return {"status": "ok", "server_time_utc": datetime.utcnow().isoformat() + "Z"}

    @app.get("/topics")
    def list_topics():
        try:
            result = client.execute("""
                SELECT 
                    topic,
                    count(*) as total_messages,
                    min(ts) as first_message,
                    max(ts) as last_message,
                    count(DISTINCT batch_id) as batch_count,
                    countIf(features != '') as messages_with_features,
                    countIf(enhanced_features != '') as messages_with_enhanced_features,
                    countIf(sliding_features != '') as messages_with_sliding_features
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
                    "batch_count": row[4],
                    "messages_with_features": row[5],
                    "messages_with_enhanced_features": row[6],
                    "messages_with_sliding_features": row[7]
                })
            
            return {"topics": topics}
        except Exception as e:
            return {"error": str(e)}, 500

    @app.get("/features/enhanced")
    def enhanced_feature_stats():
        try:
            result = client.execute("""
                SELECT 
                    topic,
                    count(*) as total_messages,
                    countIf(enhanced_features != '') as enhanced_count,
                    countIf(sliding_features != '') as sliding_count,
                    countIf(correlation_features != '') as correlation_count,
                    avg(JSONExtractFloat(enhanced_features, 'messages_per_minute')) as avg_msg_rate,
                    avg(JSONExtractFloat(enhanced_features, 'error_rate')) as avg_error_rate,
                    min(ts) as earliest_message,
                    max(ts) as latest_message
                FROM messages
                WHERE processed_at >= now() - INTERVAL 1 HOUR
                GROUP BY topic
                ORDER BY total_messages DESC
            """)
            
            topic_stats = []
            for row in result:
                topic_stats.append({
                    "topic": row[0],
                    "total_messages": row[1],
                    "enhanced_count": row[2],
                    "sliding_count": row[3],
                    "correlation_count": row[4],
                    "avg_msg_rate": round(row[5], 2) if row[5] else 0,
                    "avg_error_rate": round(row[6], 4) if row[6] else 0,
                    "earliest_message": row[7].isoformat() + "Z" if row[7] else None,
                    "latest_message": row[8].isoformat() + "Z" if row[8] else None
                })
            
            return {
                "time_window": "last 1 hour",
                "topic_breakdown": topic_stats
            }
                
        except Exception as e:
            return {"error": str(e)}, 500

    @app.get("/trends/<topic>")
    def get_trends(topic):
        try:
            result = client.execute("""
                SELECT 
                    window_start,
                    messages_per_minute,
                    error_rate,
                    trend_coefficient,
                    avg_duration_ms
                FROM aggregated_features
                WHERE topic = %(topic)s
                AND window_start >= now() - INTERVAL 6 HOUR
                ORDER BY window_start DESC
                LIMIT 50
            """, {'topic': topic})
            
            trends = []
            for row in result:
                trends.append({
                    "window_start": row[0].isoformat() + "Z",
                    "messages_per_minute": round(row[1], 2) if row[1] else 0,
                    "error_rate": round(row[2], 4) if row[2] else 0,
                    "trend_coefficient": round(row[3], 6) if row[3] else 0,
                    "avg_duration_ms": round(row[4], 2) if row[4] else 0
                })
            
            return {"topic": topic, "trends": trends}
            
        except Exception as e:
            return {"error": str(e)}, 500

    return app