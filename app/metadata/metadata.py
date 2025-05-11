import binascii
import datetime
import os.path
import uuid
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from itertools import chain
from typing import Self, Optional

import app.server
from app.server.server_args import ServerArguments


MSB_SET_MASK = 0b10000000
REMOVE_MSB_MASK = 0b01111111



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
class FeatureLevelRecord:
    frame_version: int
    type: int
    name_length: int
    name: str
    feature_level: int
    tagged_field_count: int

@dataclass
class TopicRecord:
    frame_version: int
    type: int
    version: int
    name_length: int
    topic_name: str
    topic_uuid: uuid.UUID
    tagged_field_count: int


@dataclass
class PartitionRecord:
    frame_version: int
    type: int
    version: int
    partition_id: int
    topic_uuid: uuid.UUID
    replica_length: int
    replica_array: list[int]
    length_in_sync_replica_array: int
    in_sync_replica_array: list[int]
    length_of_removing_replicas_array: int
    length_of_adding_replicas_array: int
    leader: int
    leader_epoch: int
    partition_epoch: int
    length_directories_array: int
    directories_array: list[uuid.UUID]
    tagged_field_counts: int


@dataclass
class RecordBatch:
    base_offset: int
    batch_length: int
    partition_leader_epoch: int
    magic_byte: int
    crc: int
    compression: Compression
    timestamp_type: int
    is_transactional: bool
    is_control_batch: bool
    has_delete_horizon: bool
    last_offset_delta: int
    base_timestamp: Optional[datetime.datetime]
    max_timestamp: Optional[datetime.datetime]
    producer_id: int
    producer_epoch: int
    base_sequence: int
    records_length: int
    records: list[PartitionRecord | TopicRecord | FeatureLevelRecord]


@dataclass
class ClusterMetaDataLog:
    record_batches: list[RecordBatch]

    @classmethod
    def of(file_name) -> Self:
        with open(file_name, 'rb') as in_file:
            stuff = in_file.read()
            return ClusterMetaDataLog.of_bytes(stuff)

    @classmethod
    def of_bytes(cls, stuff: bytes):

        parser: _Parser = _Parser(stuff)
        record_batches = []
        while parser.has_next():

            base_offset = parser.read(8)

            batch_length = parser.read(4)
            partition_leader_epoch = parser.read(4)
            magic_byte = parser.read(1)
            crc = parser.read(4, signed=True)
            attribues = parser.read(2)
            compression = Compression.of(attribues & 0x0003)
            timestamp_type = attribues & 0x0004
            is_transactional = (attribues & 0x0008) > 0
            is_control_batch = (attribues & 0x000f) > 0
            has_delete_horizon = (attribues & 0x0010) > 0
            last_offset_data = parser.read(4)

            timestamp_raw = parser.read(8,)
            base_timestamp= None
            timestamp_raw = parser.read(8)
            max_timestamp= None
            producer_id = parser.read(8, signed=True)
            producer_epoch = parser.read(2, signed=True)
            base_sequence = parser.read(4, signed=True)
            records_length = parser.read(4)
            records: list[TopicRecord | PartitionRecord | FeatureLevelRecord] = list()
            for i in range(records_length):
                record_length = parser.read_zig_zag(signed=True)
                attributes = parser.read(1)
                timestamp_delta = parser.read_zig_zag(signed=True)
                offset_delta = parser.read(1, signed=True)
                key_length = parser.read_zig_zag(signed=True)
                if key_length >= 0:
                    key = parser.read(key_length)
                value_length = parser.read_zig_zag(signed=True)
                frame_version = parser.read(1)
                value_type = parser.read(1)
                value = parser.parse_record(frame_version, value_type)
                records.append(value)
                headers_array_count = parser.read_zig_zag()
            val = RecordBatch(base_offset, batch_length, partition_leader_epoch, magic_byte, crc, compression, timestamp_type, is_transactional, is_control_batch, has_delete_horizon, last_offset_data, base_timestamp, max_timestamp, producer_id, producer_epoch, base_sequence, 0, records)
            record_batches.append(val)
        return ClusterMetaDataLog(record_batches)



def read_partition(server_args: ServerArguments) -> ClusterMetaDataLog:
    if os.path.exists(server_args.properties_path):
        with open(server_args.properties_path, 'rb') as in_file:
            return ClusterMetaDataLog.of_bytes(in_file.read())
    default_string = '00000000000000000000004f0000000102b069457c00000000000000000191e05af81800000191e05af818ffffffffffffffffffffffffffff000000013a000000012e010c00116d657461646174612e76657273696f6e001400000000000000000001000000e4000000010224db12dd00000000000200000191e05b2d1500000191e05b2d15ffffffffffffffffffffffffffff000000033c00000001300102000473617a00000000000040008000000000000091000090010000020182010103010000000000000000000040008000000000000091020000000102000000010101000000010000000000000000021000000000004000800000000000000100009001000004018201010301000000010000000000004000800000000000009102000000010200000001010100000001000000000000000002100000000000400080000000000000010000'
    stuff = binascii.unhexlify(default_string)
    return ClusterMetaDataLog.of_bytes(stuff)


class _Parser:

    def __init__(self, stuff: bytes):
        self.stuff = stuff
        self.index = 0

    def read(self, n: int, signed=False) -> int:
        res: int = int.from_bytes(self.stuff[self.index: self.index + n], signed=signed)
        self.index += n
        return res
    def read_zig_zag(self,signed=False) -> int:
        shift = 0
        value = 0
        aux = MSB_SET_MASK
        index = self.index
        record = b""
        while aux & MSB_SET_MASK:
            aux = self.stuff[index]
            record += aux.to_bytes()
            value += (aux & REMOVE_MSB_MASK) << shift
            index += 1
            shift += 7
        if signed:
            lsb = value & 0x01
            if lsb:
                value = -1 * ((value + 1) >> 1)
            else:
                value = value >> 1
        self.index = index
        return value
        # https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
    # https://github.com/fmoo/python-varint/blob/master/varint.py

    def read_string(self, n: int) -> str:
        res: str = self.stuff[self.index: self.index + n].decode(app.server.ENCODING)
        self.index += n
        return res

    def parse_uuid(self) -> uuid.UUID:
        res = uuid.UUID(bytes=self.stuff[self.index: self.index + 16])
        self.index += 16
        return  res
    def parse_record(self, frame_version: int, type:int) -> TopicRecord | PartitionRecord | FeatureLevelRecord:
        if type == 12:
            version = self.read(1)
            name_length = self.read_zig_zag(signed=False)
            name = self.read_string(name_length -1)
            feature_level = self.read(2)
            tagged_field_counts = self.read_zig_zag(signed=False)
            return FeatureLevelRecord(frame_version, type, name_length, name, feature_level, tagged_field_counts)
        if type == 2:
            version = self.read(1)
            name_length = self.read_zig_zag(signed=False)
            name = self.read_string(name_length - 1)
            topic_uuid = self.parse_uuid()
            tagged_field_count = self.read_zig_zag(signed=False)
            return TopicRecord(frame_version, type, version,  name_length, name, topic_uuid, tagged_field_count)
        if type == 3:
            version = self.read(1)
            partition_id = self.read(4)
            topic_uuid = self.parse_uuid()
            length_replica_array = self.read(1)
            replica_array = [            ]
            for i in range(length_replica_array -1):
                replica_array.append(self.read(4))
            length_in_sync_replica_array = self.read(1)
            in_sync_replica_array = []
            for i in range(length_in_sync_replica_array -1):
                in_sync_replica_array.append(self.read(4))
            length_removing_replicas_array = self.read(1)
            length_adding_replicas_array = self.read(1)
            leader = self.read(4)
            leader_epoch = self.read(4)
            partition_epoch = self.read(4)
            length_directories_array = self.read(1)
            directories = []
            for i in range(length_directories_array -1):
                directories.append(self.parse_uuid())
            tagged_field_counts = self.read(1)

            return PartitionRecord(frame_version, type, version, partition_id, topic_uuid, length_replica_array,
                                   replica_array, length_in_sync_replica_array, in_sync_replica_array, length_removing_replicas_array, length_adding_replicas_array, leader, leader, leader_epoch, length_directories_array, directories, tagged_field_counts)

    def has_next(self):
        return self.index < len(self.stuff)


def get_topic_stuff(log: ClusterMetaDataLog, topic_names: list[str]) -> list[PartitionRecord]:
    batches = [x for x in log.record_batches]
    records = []
    records_by_id = defaultdict(list)
    name_to_id = {}

    for batch in batches:
        for batch_record in batch.records:

            if isinstance(batch_record, TopicRecord):
                records_by_id[batch_record.topic_uuid].append(batch_record)
                name_to_id[batch_record.topic_name] = batch_record.topic_uuid
            if isinstance(batch_record, PartitionRecord):
                records_by_id[batch_record.topic_uuid].append(batch_record)

            records.append(batch_record)
    if topic_names[0] not in name_to_id:
        return []
    relevant_records = [x for x in records_by_id[name_to_id[topic_names[0]]] if isinstance(x, PartitionRecord)]
    print("lol")
    return relevant_records


