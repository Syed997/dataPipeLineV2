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
from flink.operator_functions.metric_preprocessor_operator_function import MetricPreprocessorOperatorFunction


def metric_preprocessor_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Will not exist in PROD, should be controlled by manifest

    # -----------------------------
    # Kafka Source
    # -----------------------------
    kafka_metric_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_topics("demo-laravel-metrics")
        .set_group_id("demo-laravel-metrics-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_metric_stream = env.from_source(
        kafka_metric_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source",
    )

    # -----------------------------
    # Transformation
    # -----------------------------
    processed_metric_stream = (
        raw_metric_stream
        .process(MetricPreprocessorOperatorFunction())
    )

    # -----------------------------
    # Kafka Sink
    # -----------------------------
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("processed-metrics")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    processed_metric_stream.sink_to(sink)

    env.execute("pyflink-kafka-datastream-demo")


if __name__ == "__main__":
    metric_preprocessor_job()
