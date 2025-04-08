import socket  # noqa: F401
import struct


def create_message(id):

    id_bytes = id.to_bytes(4, byteorder="big")
    return len(id_bytes).to_bytes(4, byteorder="big") + id_bytes

def handle_client(client: socket):
    client.recv(1024)
    client.sendall(create_message(7))
    client.close()


def main():

    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage

    #

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    accepted_socket, _ = server.accept()  # wait for client

    while msg := accepted_socket.recv(1024):

        accepted_socket.sendall(struct.pack(">II", 0, 7))



if __name__ == "__main__":
    main()
