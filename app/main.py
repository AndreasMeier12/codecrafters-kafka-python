import socket  # noqa: F401
import struct
import sys
import threading
from dataclasses import dataclass
import pathlib
from enum import Enum

API_VERSION_MIN_VERSION = 0
API_VERSION_MAX_VERSION = 4
API_VERSION = 18

THROTTLE_TIME_MS = 4


class ApiKeys(Enum):
    API_VERSION_REQUEST = 18, 0, 4
    DESCRIBE_TOPIC_PARTITIONS = 75, 0, 0

    def __new__(cls, *args, **kwds):
        obj = object.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, key: int, min_version: int, max_version: int):
        self.key : int = key
        self.min_version : int = min_version
        self.max_version: int = max_version

    @staticmethod
    def get_Version(request_key: int):
        return next(x for x in ApiKeys if x.key == request_key)



@dataclass
class KafkaRequestHeader:
    message_size: int
    request_api_key: int
    request_api_version: int
    correlation_id: int

    @staticmethod
    def of(msg: bytes):
        message_size = int.from_bytes(msg[0:4], "big")
        request_api_key = int.from_bytes(msg[4:6], "big")
        request_api_version = int.from_bytes(msg[6:8], "big")
        correlation_id = int.from_bytes(msg[8:12], "big")
        return KafkaRequestHeader(message_size, request_api_key, request_api_version, correlation_id)

@dataclass()
class ServerArguments:
    properties_path: pathlib.Path


def get_version_error_number(header: KafkaRequestHeader, key: ApiKeys) -> int:
    if key.min_version <= header.request_api_version <= key.max_version:
        return 0
    return 35


def handle_request(accepted_socket: socket, server_args: ServerArguments) -> None:




    while True:
        msg = accepted_socket.recv(1024)
        num_api_keys = 2
        tag_buffer = b"\x00"
        array_size = 1

        header: KafkaRequestHeader = KafkaRequestHeader.of(msg)
        api_key = ApiKeys.get_Version(header.request_api_key)
        error_bytes = get_version_error_number(header, api_key).to_bytes(2, "big", signed=False)

        message_bytes = header.correlation_id.to_bytes(4, byteorder="big", signed=False)
        message_bytes += error_bytes
        message_bytes += int(3).to_bytes(1, byteorder="big", signed=False)
        for key in ApiKeys:
            message_bytes += key.key.to_bytes(2, byteorder="big", signed=False)
            message_bytes += key.min_version.to_bytes(2, byteorder="big", signed=False)
            message_bytes += key.max_version.to_bytes(2, byteorder="big", signed=False)
            message_bytes += tag_buffer

        message_bytes += THROTTLE_TIME_MS.to_bytes(4, byteorder="big", signed=False)

        message_bytes += tag_buffer
        req_len = len(message_bytes).to_bytes(4, byteorder="big", signed=False)

        response = req_len + message_bytes

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
