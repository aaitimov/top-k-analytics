from datetime import datetime
from uuid import UUID, uuid4

import numpy as np
from pydantic import BaseModel, Field, field_serializer


class SongPlayEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    song_id: int
    duration: float
    event_ts: datetime = Field(default_factory=datetime.utcnow)

    @field_serializer("event_ts", when_used="json")
    def datetime_to_isoformat(self, timestamp: datetime) -> str:
        return timestamp.isoformat()


class SongPlayEventGenerator:
    def __init__(self, num_songs: int, zipf_param=1.2):
        self.num_songs = num_songs
        self.zipf_param = zipf_param

    def generate(self) -> SongPlayEvent:
        while True:
            song_id = np.random.zipf(self.zipf_param)
            if 1 <= song_id <= self.num_songs:
                break

        event_id = uuid4()
        event_ts = datetime.now()
        duration = np.random.uniform(0.1, 1.0)

        return SongPlayEvent(
            event_id=event_id,
            song_id=song_id,
            duration=duration,
            event_ts=event_ts,
        )
