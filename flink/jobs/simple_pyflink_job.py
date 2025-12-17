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
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Add Kafka connector JAR
    # current_dir = Path(__file__).resolve().parent
    # kafka_jar_path = current_dir / "flink-sql-connector-kafka-3.3.0-1.20.jar"
    # env.add_jars(f"file://{kafka_jar_path.as_posix()}")

    # -----------------------------
    # Kafka Source
    # -----------------------------
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("rough-input")
        .set_group_id("pyflink-demo-group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # -----------------------------
    # Kafka Sink
    # -----------------------------
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("rough-output")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # -----------------------------
    # Pipe input -> output (no changes)
    # -----------------------------
    (
        env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            "kafka-source",
            Types.STRING(),
        )
        .sink_to(sink)
    )

    env.execute("pyflink-kafka-connectivity-test")


if __name__ == "__main__":
    main()
