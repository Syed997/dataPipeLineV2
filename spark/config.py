import os

def get_config():
    return {
        'BOOTSTRAP': os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092"),
        'TOPIC_PATTERN': os.environ.get("TOPIC_PATTERN", "demo-.*"),
        'API_PORT': int(os.environ.get("API_PORT", "5001")),
        'CLICKHOUSE_HOST': os.environ.get("CLICKHOUSE_HOST", "localhost"),
        'CLICKHOUSE_PORT': int(os.environ.get("CLICKHOUSE_PORT", "9000")),
        'CLICKHOUSE_DB': os.environ.get("CLICKHOUSE_DB", "kafka_logs"),
        'ENABLE_CSV_EXPORT': os.getenv('ENABLE_CSV_EXPORT', 'true').lower() == 'true',
        'CSV_OUTPUT_DIR': os.getenv('CSV_OUTPUT_DIR', './exports'),
        'CSV_EXPORT_LIMIT': int(os.getenv('CSV_EXPORT_LIMIT', 100000)) if os.getenv('CSV_EXPORT_LIMIT') else None,
        'CSV_EXPORT_INTERVAL': int(os.getenv('CSV_EXPORT_INTERVAL', 120)),
        'CSV_CLEANUP_KEEP_COUNT': int(os.getenv('CSV_CLEANUP_KEEP_COUNT', 5)),
        'CSV_OUTPUT_DIR': './exports',
        'CSV_EXPORT_LIMIT': 1000,
        'CSV_START_DATE': '2025-09-01',
        'CSV_END_DATE': '2025-09-17',
        'CSV_KEEP_COUNT': 10
    }