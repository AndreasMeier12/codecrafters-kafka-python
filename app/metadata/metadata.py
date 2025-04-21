import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Self

from PIL.ImageChops import offset


class Compression(Enum):
    id: int

    NO_COMPRESSION = 0
    GZIP = 1
    SNAPPY = 2
    LZ4 = 3
    ZSTD = 4

    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, id:int):
        self.id: int = id

    @classmethod
    def of(cls, a: int) -> Self:
        return next((x for x in Compression if x.id == a))

@dataclass
class Record:
    attributes: int
    timestamp_delta: int
    offset_delta: int


@dataclass
class ClusterMetaDataLog:
    base_offset: int
    batch_length: int
    partition_leader_epoch: int
    magic_byte: int
    crc: int
    compression: Compression
    base_timestamp: datetime.datetime
    max_timestamp: datetime.datetime
    producer_id: int

@dataclass
class RecordValue:






    @classmethod
    def of(file_name) -> Self:
        with open(file_name, 'rb') as in_file:
            stuff = in_file.read()
            return ClusterMetaDataLog.of_bytes(stuff)

    @classmethod
    def of_bytes(cls, stuff: bytes):

        parser: _Parser = _Parser(stuff)

        base_offset = parser.read(8)

        batch_length = parser.read(4)
        partition_leader_epoch = parser.read(4)
        magic_byte = parser.read(1)
        crc = parser.read(4)
        record_batch = parser.read(2)
        compression = Compression.of(record_batch & 0x0003)
        timestamp_type = record_batch & 0x0004
        is_transactional = (record_batch & 0x0008) > 0
        is_control_batch = (record_batch & 0x000f) > 0
        has_delete_horizon = (record_batch & 0x0010) > 0
        last_offset_data = parser.read(4)

        timestamp_raw = parser.read(8)
        base_timestamp: datetime.datetime.fromtimestamp(timestamp_raw)
        timestamp_raw = parser.read(8)
        max_timestamp: datetime.datetime.fromtimestamp(timestamp_raw)
        producer_id = parser.read(8)
        producer_epoch = parser.read(4)
        base_sequence = parser.read(4)
        records_length = parser.read(4)
        records: list[Record]
        for i in range(records_length):
            record_length = parser.read(1, signed=True)
            attributes = parser.read(1)
            timestamp_delta = parser.read(1, signed=True)
            offset_delta = parser.read(1, signed=True)
            key_length = parser.read(1, signed=True)
            key = None
            if key_length >= 0:
                key = parser.read(key_length)
            value_length = parser.read(1, signed=True)
            value = None
            if value_length > 0:
                value = parser.read(value_length)



class _Parser:

    def __init__(self, stuff: bytes):
        self.stuff = stuff
        self.index = 0

    def read(self, n: int, signed=False) -> int:
        res: int = int.from_bytes(self.stuff[self.index: self.index + n], signed=signed)
        self.index += n
        return res







