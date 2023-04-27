from distributedkvstore import kvstore
from pymemcache.client.base import Client
import socket
import time
import random
import string
import threading

# Eventual Local Read, writes to anyserver with asynchronous broadcast to every one else with a logical key respective to each key
# Linear Read/Write => TOB
# Sequential Local read, write is TOB [No master is required]


def random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def sendMessageToClient(address, message):
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientSocket.connect(address)

    clientSocket.sendall(message)
    # response = clientSocket.recv(PAYLOAD_SIZE)
    # print("Response for set key", response)
    clientSocket.close()


if __name__ == "__main__":
    PAYLOAD_SIZE = 4096

    kvs = kvstore.KVStore(
        consistency='linearizable_consistency',
        replicas=3,
        storage_directory='C:\Work\Projects\Distributed_Systems\distributed_key_value\distributedkvstore\storage\key_value'
    )

    kvs.start()

    server_addresses = kvs.get_controlet_address()

    print(server_addresses)

    threads = []

    for i in range(50):
        # Connect to a client, Connect a pymemcache client

        setKey = '@GJ0_'
        setValue = random_string(7)
        no_reply = False
        setCommand = b'set' + b' ' + setKey.encode() + b' ' + setValue.encode() + b'\r\n'
        t = threading.Thread(target=(sendMessageToClient), args=(
            random.choice(server_addresses), setCommand))
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    time.sleep(100000)
    # clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # clientSocket.connect(server_addresses[0])

    # print('get key request')
    # getKey = '@GJ0_'
    # getCommand = b'get' + b' ' + setKey.encode() + b'\r\n'
    # clientSocket.sendall(getCommand)
    # response = clientSocket.recv(PAYLOAD_SIZE)

    # print("Response for get key", response)
    # clientSocket.close()
