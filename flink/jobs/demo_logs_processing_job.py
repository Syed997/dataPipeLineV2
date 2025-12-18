"""
PyFlink Kafka Stream Processor for Laravel Logs

This module defines a minimal PyFlink DataStream job that consumes JSON log
events from a Kafka topic, enriches them, and writes the results to another
Kafka topic.

- Source topic: ``demo-laravel-logs`` (earliest offsets)
- Sink topic: ``demo-laravel-logs-processed``
- Bootstrap server: ``kafka:9092``
- Consumer group: ``pyflink-demo-laravel-group``

Processing
----------
Each Kafka record is expected to be a JSON string. The job deserializes the
record, adds a ``processed_by_flink = True`` flag, and serializes it back to
JSON.

Notes
-----
- Parallelism is set to 1.
- No watermarks or event-time semantics are used.
- Designed as a simple reference pipeline for Laravel → Kafka → Flink setups.
"""

import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy


# -----------------------------
# Simple transformation logic
# -----------------------------
def process_json(value: str) -> str:
    """
    Input:  JSON string
    Output: JSON string
    """
    data = json.loads(value)
    data["processed_by_flink"] = True
    return json.dumps(data)


# -----------------------------
# Main job
# -----------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # -----------------------------
    # Kafka Source
    # -----------------------------
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("demo-laravel-logs")
        .set_group_id("pyflink-demo-laravel-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source",
    )

    # -----------------------------
    # Transformation
    # -----------------------------
    processed_stream = stream.map(
        process_json,
        output_type=Types.STRING(),
    )

    # -----------------------------
    # Kafka Sink
    # -----------------------------
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("demo-laravel-logs-processed")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    processed_stream.sink_to(sink)

    env.execute("pyflink-demo-laravel-logs-processor")


if __name__ == "__main__":
    main()
