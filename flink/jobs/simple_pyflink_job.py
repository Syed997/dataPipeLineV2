import json
from pathlib import Path

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

    # very simple change
    data["processed"] = True
    data["value"] = data.get("value", 0) * 2

    return json.dumps(data)


# -----------------------------
# Main job
# -----------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Add the Kafka connector JAR (located in the same directory as this job) to Flink’s runtime classpath
    current_dir = Path(__file__).resolve().parent
    kafka_jar_path = current_dir / "flink-sql-connector-kafka-3.3.0-1.20.jar"
    env.add_jars(f"file://{kafka_jar_path.as_posix()}")

    # -----------------------------
    # Kafka Source
    # -----------------------------
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("input-events")
        .set_group_id("pyflink-demo-group")
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
    processed_stream = (
        stream
        .map(
            process_json,
            output_type=Types.STRING(),
        )
    )

    # -----------------------------
    # Kafka Sink
    # -----------------------------
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("output-events")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    processed_stream.sink_to(sink)

    env.execute("pyflink-kafka-datastream-demo")


if __name__ == "__main__":
    main()
