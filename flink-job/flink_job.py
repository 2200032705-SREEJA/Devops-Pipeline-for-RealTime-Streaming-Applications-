"""
Flink Spotify Stream Processor
--------------------------------
PyFlink job that reads from Kafka topic 'spotify-stream',
aggregates top artists and listening duration in a 60-second
tumbling window, and prints results to stdout.

Run via Flink CLI:
    flink run -py flink_job.py
"""
import json
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Time


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "spotify-stream")


def parse_event(raw: str):
    """Parse JSON string to (artist, duration_ms) tuple."""
    try:
        event = json.loads(raw)
        return event.get("artist_name", "Unknown"), int(event.get("duration_ms", 0))
    except Exception:
        return None


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("flink-consumer-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    # Parse → filter nulls → key by artist → window → aggregate
    results = (
        stream
        .map(parse_event)
        .filter(lambda x: x is not None)
        .key_by(lambda x: x[0])
        .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
        .map(lambda x: f"🎵 Artist: {x[0]:<40} | Total ms: {x[1]:>10,} | Minutes: {x[1]/60000:>6.1f}")
    )

    results.print()
    env.execute("Spotify Stream Processor")


if __name__ == "__main__":
    main()
