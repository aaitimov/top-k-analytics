import json
from datetime import datetime
from unittest.mock import patch
from uuid import UUID

from freezegun import freeze_time

from app.generator import SongPlayEvent, SongPlayEventGenerator


def test_generate_event_fields():
    expected = SongPlayEvent(
        event_id=UUID("75477e39-666a-4ff0-9018-48359771cde3"),
        song_id=1001,
        duration=0.5,
        event_ts=datetime(2025, 7, 30, 1, 10),
    )

    with patch("app.generator.uuid4", return_value=expected.event_id), patch(
        "app.generator.np.random.zipf", return_value=expected.song_id
    ), patch(
        "app.generator.np.random.uniform", return_value=expected.duration
    ), freeze_time(
        expected.event_ts
    ):
        generator = SongPlayEventGenerator(num_songs=100_000)
        actual = generator.generate()

        assert actual == expected


def test_event_is_serializable_to_json():
    expected = {
        "event_id": "75477e39-666a-4ff0-9018-48359771cde3",
        "song_id": 1001,
        "duration": 0.5,
        "event_ts": "2025-07-30T01:10:40.123",
    }

    event = SongPlayEvent(
        event_id=UUID(expected["event_id"]),
        song_id=expected["song_id"],
        duration=expected["duration"],
        event_ts=datetime(2025, 7, 30, 1, 10, 40, 123456),
    )

    actual = json.loads(event.model_dump_json())

    assert actual == expected
