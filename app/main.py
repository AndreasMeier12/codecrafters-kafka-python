import socket  # noqa: F401
import sys
import threading
import uuid
from dataclasses import dataclass
import pathlib
from enum import Enum
from typing import Callable, Self
from app.metadata import metadata
from app.metadata.metadata import get_topic_stuff
from app.server import ENCODING
from app.server.server_args import ServerArguments

API_VERSION_MIN_VERSION = 0
API_VERSION_MAX_VERSION = 4
API_VERSION = 18

THROTTLE_TIME_MS = 4

TAG_BUFFER = b"\x00"


@dataclass
class KafkaRequestHeader:
    message_size: int
    request_api_key: int
    request_api_version: int
    correlation_id: int
    payload: bytes
    raw_msg: bytes

    @classmethod
    def of(cls, msg: bytes) -> Self:
        message_size = int.from_bytes(msg[0:4], "big")
        request_api_key = int.from_bytes(msg[4:6], "big")
        request_api_version = int.from_bytes(msg[6:8], "big")
        correlation_id = int.from_bytes(msg[8:12], "big")
        payload = msg[12:]


        return KafkaRequestHeader(message_size, request_api_key, request_api_version, correlation_id, payload, msg)


@dataclass
class DescribeTopicPartition:
    topic_id: uuid
    partition: None
    partitions_array_length: int
    response_partition_limit: int
    topic_names: list[str]
    cursor: int
    error_code: int
    is_internal: bool = False

    operations_allowed = int(0x00000df8).to_bytes(4)

    @classmethod
    def from_bytes(cls, stuff: bytes, server_args: ServerArguments):
        metadata_log = metadata.read_partition(server_args)
        length_raw = stuff[0:2] # 12-14
        length = int.from_bytes(length_raw, "big", signed=False)
        client_id = stuff[2: 2 + length].decode(ENCODING)
        index = 2 + length
        array_length = int.from_bytes(stuff[index:index+2], signed=False) -1
        topics = []
        index = index + 2
        print(f"{length=}, {array_length=}")
        cursor = 0
        response_partition_limit = 0
        for i in range(array_length):
            topic_length = int.from_bytes(stuff[index: index + 1], signed=False) -1
            index = index + 1
            topic_name = stuff[index: index + topic_length].decode(ENCODING)
            index = index + topic_length
            topics.append(topic_name)
            num_partitions_limit = int.from_bytes(stuff[index: index + 4])
            index = index + 4

            cursor = int.from_bytes(stuff[index:index+1], signed=False)
            index = index + 1

            response_partition_limit_raw = stuff[index: index + 4]
            response_partition_limit = int.from_bytes(response_partition_limit_raw)
            index += 4
        get_topic_stuff(metadata_log, topics)



        topic_id: uuid = uuid.UUID('00000000-0000-0000-0000-000000000000')
        return DescribeTopicPartition(topic_id, None, array_length, response_partition_limit, topics, cursor)

    def serialize(self, request: KafkaRequestHeader) -> bytes:
        buf = "".encode(ENCODING)
        buf += request.correlation_id.to_bytes(4, byteorder="big", signed=False)
        buf += TAG_BUFFER
        buf += (0).to_bytes(4, byteorder="big", signed=False)
        buf += (len(self.topic_names) + 1).to_bytes(1, byteorder="big", signed=False)
        buf += (self.error_code).to_bytes(2, byteorder="big", signed=False)


        print(f"{self.topic_names=}")

        topic = self.topic_names[0]
        print(f"{topic=}, reported topic length={len(topic.encode(ENCODING)) + 1}")
        length = int(len(topic.encode(ENCODING)) + 1).to_bytes()
        buf = buf + length
        buf = buf + topic.encode(ENCODING)
        buf = buf + int(0).to_bytes(16)
        is_internal_msg = int(0).to_bytes(1)
        buf = buf + is_internal_msg
        buf = buf + self.partitions_array_length.to_bytes(1)
        buf = buf + self.operations_allowed   #
        buf = buf +  TAG_BUFFER #
        print(len(buf.hex()))
        print(f"{self.cursor=}")
        buf = buf + (255).to_bytes(1, byteorder="big", signed=False)
        buf = buf + TAG_BUFFER
        return  buf









@dataclass()
class KafkaResponse:
    error_code: int
    body: bytes


def handle_api_version(request: KafkaRequestHeader, server_args: ServerArguments) -> KafkaResponse:
    api_key = ApiKeys.get_Version(request.request_api_key)
    error_bytes = get_version_error_number(request, api_key).to_bytes(2, "big", signed=False)

    message_bytes = request.correlation_id.to_bytes(4, byteorder="big", signed=False)
    message_bytes += error_bytes
    if api_key.key != 75:
        message_bytes += int(3).to_bytes(1, byteorder="big", signed=False)
        for key in ApiKeys:
            message_bytes += key.key.to_bytes(2, byteorder="big", signed=False)
            message_bytes += key.min_version.to_bytes(2, byteorder="big", signed=False)
            message_bytes += key.max_version.to_bytes(2, byteorder="big", signed=False)
            message_bytes += TAG_BUFFER

    message_bytes += THROTTLE_TIME_MS.to_bytes(4, byteorder="big", signed=False)

    message_bytes += TAG_BUFFER
    return KafkaResponse(0, message_bytes)

def compare_byteroos(a: bytes, b: bytes):
    if len(a.hex()) != len(b.hex()):
        print(f"lengths differ: {len(a.hex())} to {len(b.hex())}")

    for (i, pair) in enumerate(zip(a.hex(),b.hex())):
        if pair[0] != pair[1]:
            print(f'pos {i}: {pair[0]}, {pair[1]}')




def handle_describe_topic_partition(request: KafkaRequestHeader, server_args: ServerArguments):
    lolzers = DescribeTopicPartition.from_bytes(request.payload, server_args)
    response_header = request.correlation_id.to_bytes(4, byteorder="big", signed=False)

    return KafkaResponse(3, lolzers.serialize(request))






class ApiKeys(Enum):
    handler: Callable[[KafkaRequestHeader, ServerArguments], KafkaResponse]
    API_VERSION_REQUEST = 18, 0, 4, handle_api_version
    DESCRIBE_TOPIC_PARTITIONS = 75, 0, 0, handle_describe_topic_partition

    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, key: int, min_version: int, max_version: int, handler: Callable[[KafkaRequestHeader,
                                                                                        ServerArguments], KafkaResponse]):
        self.key: int = key
        self.min_version: int = min_version
        self.max_version: int = max_version
        self.handler = handler

    @staticmethod
    def get_Version(request_key: int):
        return next(x for x in ApiKeys if x.key == request_key)


def get_version_error_number(header: KafkaRequestHeader, key: ApiKeys) -> int:
    if key.min_version <= header.request_api_version <= key.max_version:
        return 0
    return 35


def handle_request(accepted_socket: socket, server_args: ServerArguments) -> None:




    while True:
        msg = accepted_socket.recv(1024)
        num_api_keys = 2
        array_size = 1
        print(msg.hex())


        header: KafkaRequestHeader = KafkaRequestHeader.of(msg)
        api_key = ApiKeys.get_Version(header.request_api_key)

        kafka_response = api_key.handler(header, server_args)


        req_len = len(kafka_response.body).to_bytes(4, byteorder="big", signed=False)

        response = req_len + kafka_response.body

        accepted_socket.sendall(response)


def main():

    print("Logs from your program will appear here!")
    properties_path = pathlib.Path(sys.argv[1])
    server_args = ServerArguments(properties_path)

    # Uncomment this to pass the first stage

    #

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        accepted_socket, _ = server.accept()
        # wait for client
        threading.Thread(target=handle_request, args=(accepted_socket, server_args)).start()




if __name__ == "__main__":
    main()
