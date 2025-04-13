import socket  # noqa: F401
import struct
import threading
from dataclasses import dataclass

MIN_VERSION = 0
MAX_VERSION = 4
API_VERSION = 18

THROTTLE_TIME_MS = 4

def create_message(id):

    id_bytes = id.to_bytes(4, byteorder="big")
    return len(id_bytes).to_bytes(4, byteorder="big") + id_bytes

def handle_client(client: socket):
    client.recv(1024)
    client.sendall(create_message(7))
    client.close()

@dataclass
class KafkaHeader:
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
        return KafkaHeader(message_size, request_api_key, request_api_version, correlation_id)

def get_version_error_number(header: KafkaHeader) -> int:
    if MIN_VERSION <= header.request_api_version <= MAX_VERSION:
        return 0
    return 35

def handle_request(accepted_socket: socket) -> None:
    while True:
        msg = accepted_socket.recv(1024)
        num_api_keys = 2
        tag_buffer = b"\x00"
        array_size = 1

        header: KafkaHeader = KafkaHeader.of(msg)
        error_bytes = get_version_error_number(header).to_bytes(2, "big", signed=False)

        message_bytes = header.correlation_id.to_bytes(4, byteorder="big", signed=False)
        message_bytes += error_bytes
        message_bytes += int(2).to_bytes(1, byteorder="big", signed=False)
        message_bytes += header.request_api_key.to_bytes(2, byteorder="big", signed=False)
        message_bytes += MIN_VERSION.to_bytes(2, byteorder="big", signed=False)
        message_bytes += MAX_VERSION.to_bytes(2, byteorder="big", signed=False)
        message_bytes += tag_buffer

        message_bytes += THROTTLE_TIME_MS.to_bytes(4, byteorder="big", signed=False)

        message_bytes += tag_buffer
        req_len = len(message_bytes).to_bytes(4, byteorder="big", signed=False)

        response = req_len + message_bytes

        accepted_socket.sendall(response)


def main():

    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage

    #

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        accepted_socket, _ = server.accept()
        # wait for client
        threading.Thread(target=handle_request, args=(accepted_socket,)).start()




if __name__ == "__main__":
    main()
