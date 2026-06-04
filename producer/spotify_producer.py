"""
Spotify Kafka Producer
----------------------
Reads Spotify streaming history CSV and sends each row
as a real-time event to a Kafka topic.

Usage:
    python spotify_producer.py --file data/spotify_data.csv --topic spotify-stream
"""
import json
import time
import argparse
import csv
from kafka import KafkaProducer
from datetime import datetime


def create_producer(bootstrap_servers='localhost:9092'):
    """Create and return a Kafka producer."""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        retries=5,
        retry_backoff_ms=500,
    )
    print(f"✅ Connected to Kafka broker at {bootstrap_servers}")
    return producer


def send_spotify_data(producer, file_path, topic, delay=0.01):
    """
    Read CSV file and send each row as a Kafka message.

    Args:
        producer: KafkaProducer instance
        file_path: path to Spotify CSV file
        topic: Kafka topic name
        delay: seconds between messages (simulates real-time)
    """
    sent = 0
    errors = 0

    print(f"📂 Reading file: {file_path}")
    print(f"📤 Sending to topic: {topic}\n")

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                event = {
                    "timestamp": row.get("ts") or row.get("timestamp") or datetime.utcnow().isoformat(),
                    "track_name": row.get("master_metadata_track_name") or row.get("track_name", "Unknown"),
                    "artist_name": row.get("master_metadata_album_artist_name") or row.get("artist_name", "Unknown"),
                    "album_name": row.get("master_metadata_album_album_name") or row.get("album_name", "Unknown"),
                    "duration_ms": int(row.get("ms_played") or row.get("duration_ms") or 0),
                    "platform": row.get("platform", "unknown"),
                    "event_time": datetime.utcnow().isoformat()
                }

                key = event["artist_name"]
                producer.send(topic, key=key, value=event)
                sent += 1

                if sent % 1000 == 0:
                    print(f"  📨 Sent {sent} messages...")

                time.sleep(delay)

            except Exception as e:
                errors += 1
                print(f"  ⚠️  Error on row {sent + errors}: {e}")

    producer.flush()
    print(f"\n✅ Done! Sent: {sent} | Errors: {errors}")
    return sent


def main():
    parser = argparse.ArgumentParser(description='Spotify Kafka Producer')
    parser.add_argument('--file', default='data/spotify_data.csv', help='Path to Spotify CSV file')
    parser.add_argument('--topic', default='spotify-stream', help='Kafka topic name')
    parser.add_argument('--broker', default='localhost:9092', help='Kafka broker address')
    parser.add_argument('--delay', type=float, default=0.01, help='Delay between messages in seconds')
    args = parser.parse_args()

    print("=" * 50)
    print("  🎵 Spotify Real-Time Kafka Producer")
    print("=" * 50)

    producer = create_producer(args.broker)
    send_spotify_data(producer, args.file, args.topic, args.delay)
    producer.close()


if __name__ == '__main__':
    main()
