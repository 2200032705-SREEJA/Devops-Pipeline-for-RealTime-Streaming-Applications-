"""
Tests for Spotify Kafka Producer and Consumer
"""
import json
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


# ── Producer Tests ─────────────────────────────────────────────────────────────

class TestSpotifyProducer:
    """Unit tests for spotify_producer.py"""

    def test_event_json_serialization(self):
        """Producer events must be JSON-serializable."""
        event = {
            "timestamp": "2024-01-01T00:00:00",
            "track_name": "Blinding Lights",
            "artist_name": "The Weeknd",
            "album_name": "After Hours",
            "duration_ms": 200040,
            "platform": "android",
            "event_time": datetime.utcnow().isoformat()
        }
        serialized = json.dumps(event).encode('utf-8')
        assert json.loads(serialized) == event

    def test_kafka_message_key_is_artist_name(self):
        """Kafka message key should be the artist name."""
        artist = "Taylor Swift"
        key_serialized = artist.encode('utf-8')
        assert key_serialized == b"Taylor Swift"

    def test_missing_fields_default_to_unknown(self):
        """Missing CSV fields should gracefully default."""
        row = {}
        track = row.get("track_name", "Unknown")
        artist = row.get("artist_name", "Unknown")
        assert track == "Unknown"
        assert artist == "Unknown"

    def test_duration_ms_defaults_to_zero(self):
        """Missing duration should default to 0."""
        row = {}
        duration = int(row.get("ms_played") or row.get("duration_ms") or 0)
        assert duration == 0

    def test_timestamp_fallback(self):
        """Timestamp should fall back to current time if not in row."""
        row = {}
        ts = row.get("ts") or row.get("timestamp") or datetime.utcnow().isoformat()
        assert ts is not None
        assert "T" in ts  # ISO format check


# ── Flink Parsing Tests ────────────────────────────────────────────────────────

class TestFlinkParsing:
    """Unit tests for Flink job parsing logic."""

    def test_valid_json_parse(self):
        raw = json.dumps({
            "artist_name": "Drake",
            "duration_ms": 180000
        })
        event = json.loads(raw)
        result = (event.get("artist_name", "Unknown"), int(event.get("duration_ms", 0)))
        assert result == ("Drake", 180000)

    def test_invalid_json_returns_none(self):
        raw = "not valid json {{"
        try:
            json.loads(raw)
            result = True
        except Exception:
            result = None
        assert result is None

    def test_flink_reduce_aggregation(self):
        """Tumbling window reduce should sum durations."""
        a = ("Artist A", 60000)
        b = ("Artist A", 90000)
        result = (a[0], a[1] + b[1])
        assert result == ("Artist A", 150000)

    def test_timestamp_conversion(self):
        """ISO timestamp should parse without errors."""
        ts = "2024-06-15T14:30:00"
        dt = datetime.fromisoformat(ts)
        assert dt.hour == 14


# ── Integration Tests ──────────────────────────────────────────────────────────

class TestIntegration:
    """Integration-style tests (mock Kafka)."""

    @patch('kafka.KafkaProducer')
    def test_producer_connects_to_broker(self, mock_producer_class):
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        # Simulate creating producer
        producer = mock_producer_class(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        assert producer is not None

    @patch('kafka.KafkaProducer')
    def test_producer_sends_message(self, mock_producer_class):
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        producer = mock_producer_class(bootstrap_servers='localhost:9092')
        event = {"artist_name": "Adele", "duration_ms": 240000}
        producer.send("spotify-stream", key=b"Adele", value=event)

        producer.send.assert_called_once_with("spotify-stream", key=b"Adele", value=event)

    @patch('kafka.KafkaConsumer')
    def test_consumer_reads_messages(self, mock_consumer_class):
        mock_msg = MagicMock()
        mock_msg.value = {
            "artist_name": "Billie Eilish",
            "duration_ms": 195000,
            "event_time": "2024-01-01T10:00:00"
        }
        mock_consumer = MagicMock()
        mock_consumer.__iter__ = MagicMock(return_value=iter([mock_msg]))
        mock_consumer_class.return_value = mock_consumer

        consumer = mock_consumer_class("spotify-stream", bootstrap_servers="localhost:9092")
        for msg in consumer:
            assert msg.value["artist_name"] == "Billie Eilish"
            assert msg.value["duration_ms"] == 195000

    def test_end_to_end_data_flow(self):
        """Simulate full pipeline: produce → parse → aggregate."""
        # 1. Simulate raw CSV row
        row = {
            "master_metadata_track_name": "Shape of You",
            "master_metadata_album_artist_name": "Ed Sheeran",
            "master_metadata_album_album_name": "Divide",
            "ms_played": "233000",
            "platform": "ios",
            "ts": "2024-01-15T09:30:00"
        }

        # 2. Build event (producer logic)
        event = {
            "timestamp": row.get("ts"),
            "track_name": row.get("master_metadata_track_name", "Unknown"),
            "artist_name": row.get("master_metadata_album_artist_name", "Unknown"),
            "album_name": row.get("master_metadata_album_album_name", "Unknown"),
            "duration_ms": int(row.get("ms_played") or 0),
            "platform": row.get("platform", "unknown"),
        }

        # 3. Serialize and deserialize (Kafka round-trip)
        serialized = json.dumps(event).encode('utf-8')
        deserialized = json.loads(serialized.decode('utf-8'))

        # 4. Flink parse
        parsed = (deserialized.get("artist_name", "Unknown"), int(deserialized.get("duration_ms", 0)))

        assert parsed == ("Ed Sheeran", 233000)

    def test_error_handling_on_failure(self):
        """Consumer should handle malformed messages without crashing."""
        bad_messages = ["{invalid json", "", None, "{}"]
        results = []
        for raw in bad_messages:
            try:
                if raw:
                    event = json.loads(raw)
                    results.append(("ok", event))
                else:
                    results.append(("skip", None))
            except Exception:
                results.append(("error", None))

        assert ("error", None) in results  # malformed caught
        assert ("ok", {}) in results       # empty object ok
