from clickhouse_driver import Client

def initialize_clickhouse(config):
    client = Client(
        host=config['CLICKHOUSE_HOST'],
        port=config['CLICKHOUSE_PORT'],
        database=config['CLICKHOUSE_DB']
    )
    
    client.execute(f"""
    CREATE TABLE IF NOT EXISTS {config['CLICKHOUSE_DB']}.messages (
        topic String,
        value String,
        ts DateTime,
        batch_id UInt64,
        window_start DateTime,
        window_end DateTime,
        features String DEFAULT '',
        enhanced_features String DEFAULT '',
        sliding_features String DEFAULT '',
        correlation_features String DEFAULT '',
        processed_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (ts, topic)
    """)

    client.execute(f"""
    CREATE TABLE IF NOT EXISTS {config['CLICKHOUSE_DB']}.aggregated_features (
        topic String,
        window_start DateTime,
        window_end DateTime,
        window_minutes UInt32,
        messages_per_minute Float64,
        error_rate Float64,
        unique_sources UInt32,
        avg_body_length Float64,
        avg_duration_ms Float64,
        p95_duration_ms Float64,
        trend_coefficient Float64,
        processed_at DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    ORDER BY (window_start, topic)
    """)
    
    return client