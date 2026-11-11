from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EmptyArgs(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WriteStatus(_message.Message):
    __slots__ = ("wrote", "firstOffset", "duplicate")
    WROTE_FIELD_NUMBER: _ClassVar[int]
    FIRSTOFFSET_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    wrote: int
    firstOffset: int
    duplicate: bool
    def __init__(self, wrote: _Optional[int] = ..., firstOffset: _Optional[int] = ..., duplicate: bool = ...) -> None: ...

class ReadParams(_message.Message):
    __slots__ = ("topic", "offset", "batchSize", "stopOnCompletion", "fromEarliest")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    BATCHSIZE_FIELD_NUMBER: _ClassVar[int]
    STOPONCOMPLETION_FIELD_NUMBER: _ClassVar[int]
    FROMEARLIEST_FIELD_NUMBER: _ClassVar[int]
    topic: str
    offset: int
    batchSize: int
    stopOnCompletion: bool
    fromEarliest: bool
    def __init__(self, topic: _Optional[str] = ..., offset: _Optional[int] = ..., batchSize: _Optional[int] = ..., stopOnCompletion: bool = ..., fromEarliest: bool = ...) -> None: ...

class InputEntries(_message.Message):
    __slots__ = ("topic", "entries", "producerId", "sequence")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    PRODUCERID_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    topic: str
    entries: _containers.RepeatedScalarFieldContainer[bytes]
    producerId: bytes
    sequence: int
    def __init__(self, topic: _Optional[str] = ..., entries: _Optional[_Iterable[bytes]] = ..., producerId: _Optional[bytes] = ..., sequence: _Optional[int] = ...) -> None: ...

class TopicList(_message.Message):
    __slots__ = ("topics",)
    TOPICS_FIELD_NUMBER: _ClassVar[int]
    topics: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, topics: _Optional[_Iterable[str]] = ...) -> None: ...

class Entry(_message.Message):
    __slots__ = ("offset", "content")
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    offset: int
    content: bytes
    def __init__(self, offset: _Optional[int] = ..., content: _Optional[bytes] = ...) -> None: ...

class OutputEntries(_message.Message):
    __slots__ = ("entries",)
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[Entry]
    def __init__(self, entries: _Optional[_Iterable[_Union[Entry, _Mapping]]] = ...) -> None: ...
