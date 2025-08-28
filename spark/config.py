import os

def get_config():
    return {
        'BOOTSTRAP': os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092"),
        'TOPIC_PATTERN': os.environ.get("TOPIC_PATTERN", "demo-.*"),
        'API_PORT': int(os.environ.get("API_PORT", "5001")),
        'CLICKHOUSE_HOST': os.environ.get("CLICKHOUSE_HOST", "localhost"),
        'CLICKHOUSE_PORT': int(os.environ.get("CLICKHOUSE_PORT", "9000")),
        'CLICKHOUSE_DB': os.environ.get("CLICKHOUSE_DB", "kafka_logs")
    }