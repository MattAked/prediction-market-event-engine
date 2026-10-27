import threading
from datetime import datetime
from dataclasses import dataclass
from uuid import uuid4
from queue import Queue


class RecordingAdapter:
    pass

@dataclass
class ActiveRecording:
    stream_ref: str
    session_id: str
    metadata: dict
    started_at: datetime

@dataclass
class Recording:
    stream_ref: str
    session_id: str
    metadata: dict
    started_at: datetime
    ended_at: datetime

@dataclass
class RecordedItem:
    stream_ref: str
    event: object
    timestamp: datetime
    sequence_num: int

class Recorder:
    def __init__(self, adapter: RecordingAdapter, batch_size=100, flush_secs=10):
        self._adapter = adapter  # Maybe an adapter for the prediction market used for order book storage
        self.batch_size = batch_size
        self.flush_secs = flush_secs  # If no write in this amount of time just process the current batch
        self._queue = Queue()
        self._queue_processor: threading.Thread | None = None
        self._sequence = 0
        self.last_write_time = datetime.now()
        self.running = False
        self.current_batch = []

        self.active_recordings: dict[str, ActiveRecording] = {}

    # When its started it should read highest sequence number and then each record increments it
    def start(self) -> None:
        if self.running:
            raise RuntimeError("Recorder is already running")

        self._queue_processor = threading.Thread(target=self._writer)
        self._queue_processor.start()
        self.running = True


    def stop(self):
        if not self.running:
            raise RuntimeError("Recorder isn't running")

        self.running = False
        if self._queue_processor:
            self._queue_processor.join(timeout=10)

        self._record_batch(self.current_batch)
        self.current_batch = []

    def register_stream(self, metadata: dict) -> str:
        stream_ref = uuid4().hex

        self.active_recordings[stream_ref] = ActiveRecording(stream_ref=stream_ref,
                                                             session_id=uuid4().hex,
                                                             metadata=metadata,
                                                             started_at=datetime.now())

        return stream_ref

    def close_stream(self, stream_ref: str) -> None:
        active_recording = self.active_recordings.pop(stream_ref)
        if active_recording:
            self._queue.put(Recording(stream_ref=active_recording.stream_ref,
                                      session_id=active_recording.session_id,
                                      metadata=active_recording.metadata,
                                      started_at=active_recording.started_at,
                                      ended_at=datetime.now()))

    def _record_session(self, recording: Recording):
        pass

    def _record_event(self, event: RecordedItem):
        pass

    def _record_batch(self, batch: list):
        pass

    def record_event(
        self,
        stream_ref: str,
        event: object,
        timestamp: datetime,
    ) -> None:
        self._queue.put_nowait(
            RecordedItem(
                stream_ref=stream_ref,
                event=event,
                timestamp=timestamp,
                sequence_num=self._sequence
            )
        )

        self._sequence += 1

    async def _writer(self):
        while self.running:
            item = self._queue.get()
            if isinstance(item, Recording):
                self._record_session(item)
            elif isinstance(item, RecordedItem):
                self._record_event(item)


            self.current_batch.append(item)

            if len(self.current_batch) >= self.batch_size or (datetime.now() - self.last_write_time).seconds > self.flush_secs:
                self._record_batch(self.current_batch)
                self.current_batch = []
                self.last_write_time = datetime.now()

