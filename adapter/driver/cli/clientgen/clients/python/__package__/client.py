"""A small client for an Ibsen server: write, read and list.

With ``producer=True`` retries are safe: the client names itself with a random producer id and
numbers its writes to each topic 1, 2, 3, and a write whose reply is lost is sent again with
the same number, which the server answers as a duplicate instead of storing twice. Writes from
one client to one topic must not overlap.
"""

import os
import threading
import time
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

import grpc

from . import ibsen_pb2, ibsen_pb2_grpc

_RETRYABLE = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.ABORTED,
    grpc.StatusCode.UNKNOWN,
}


@dataclass(frozen=True)
class WriteResult:
    """Where a write's entries are; duplicate means the server already held this write."""

    first_offset: int
    count: int
    duplicate: bool


@dataclass(frozen=True)
class Entry:
    offset: int
    content: bytes


class IbsenClient:
    def __init__(
        self,
        target: str = "localhost:54321",
        producer: bool = False,
        retries: int = 5,
        backoff: float = 0.1,
        channel: Optional[grpc.Channel] = None,
    ):
        """Connects to target without TLS, or uses channel, such as a grpc.secure_channel."""
        self._channel = channel or grpc.insecure_channel(target)
        self._stub = ibsen_pb2_grpc.IbsenStub(self._channel)
        self._producer = os.urandom(16) if producer else b""
        self._next = {}
        self._retries = retries
        self._backoff = backoff
        self._lock = threading.Lock()

    def write(self, topic: str, entries: Iterable[bytes]) -> WriteResult:
        """Writes entries to topic. With a producer, a lost reply is retried with the same number."""
        request = ibsen_pb2.InputEntries(topic=topic, entries=list(entries))
        if not self._producer:
            return self._result(self._stub.write(request))
        with self._lock:
            sequence = self._next.get(topic, 1)
            request.producerId = self._producer
            request.sequence = sequence
            wait = self._backoff
            attempt = 0
            while True:
                try:
                    status = self._stub.write(request)
                    self._next[topic] = sequence + 1
                    return self._result(status)
                except grpc.RpcError as error:
                    if error.code() not in _RETRYABLE or attempt >= self._retries:
                        raise
                    time.sleep(wait)
                    wait *= 2
                    attempt += 1

    @staticmethod
    def _result(status) -> WriteResult:
        return WriteResult(status.firstOffset, status.wrote, status.duplicate)

    def read(
        self,
        topic: str,
        offset: int = 0,
        follow: bool = False,
        from_earliest: bool = False,
        callback: Optional[Callable[[Entry], None]] = None,
    ) -> List[Entry]:
        """Reads topic from offset. Without follow it returns the entries at the end of the log;
        with it, it calls callback for each entry until the channel is closed."""
        stream = self._stub.read(
            ibsen_pb2.ReadParams(
                topic=topic,
                offset=offset,
                batchSize=1000,
                stopOnCompletion=not follow,
                fromEarliest=from_earliest,
            )
        )
        collected = []
        for batch in stream:
            for entry in batch.entries:
                read = Entry(entry.offset, entry.content)
                if callback:
                    callback(read)
                else:
                    collected.append(read)
        return collected

    def list(self) -> List[str]:
        """Names the topics the server holds."""
        return list(self._stub.list(ibsen_pb2.EmptyArgs()).topics)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
