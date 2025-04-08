import socket  # noqa: F401
import struct
from dataclasses import dataclass


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



def main():

    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage

    #

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    accepted_socket, _ = server.accept()  # wait for client

    while msg := accepted_socket.recv(1024):

        header: KafkaHeader = KafkaHeader.of(msg)
        error_code = 0 if 0<= header.request_api_version <= 4 else 35
        message = header.correlation_id.to_bytes(4, byteorder="big", signed=True)
        if error_code:
            message = message + error_code.to_bytes(2, byteorder="big", signed=True)
        message_len = len(message).to_bytes(4, byteorder="big", signed=False)
        message = message_len + message


        accepted_socket.sendall(message)



if __name__ == "__main__":
    main()
