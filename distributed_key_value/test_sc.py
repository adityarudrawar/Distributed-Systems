import threading
import string
import random
import time
import socket
from pymemcache.client.base import Client
from distributedkvstore import kvstore
import collections
collections.Callable = collections.abc.Callable

import pdb

# Eventual Local Read, writes to anyserver with asynchronous broadcast to every one else with a logical key respective to each key
# Linear Read/Write => TOB
# Sequential Local read, write is TOB [No master is required]


def random_string(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str


def customClient(address, message):
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientSocket.connect(address)

    clientSocket.sendall(message)
    # response = clientSocket.recv(PAYLOAD_SIZE)
    # print("Response for set key", response)
    clientSocket.close()


def memcacheClientSet(address, key, value):
    c = Client(address)
    response = c.set(key, value, noreply=False)
    print("SET response ", response)
    c.close()


def memcacheClientGet(address, key):
    c = Client(address)
    response = c.get(key)
    print("GET response ", response)
    c.close()


if __name__ == "__main__":
    PAYLOAD_SIZE = 4096

    kvs = kvstore.KVStore(
        consistency='sequential_consistency',
        replicas=3,
        storage_directory='C:\Work\Projects\Distributed_Systems\distributed_key_value\distributedkvstore\storage\key_value',
        output_directory='C:\Work\Projects\Distributed_Systems\distributed_key_value\distributedkvstore\output\sequential_consistency_output'
        )

    kvs.start()

    server_addresses = kvs.get_controlet_address()

    numRequests = 4

    threads = []
    for i in range(numRequests):
        setKey = random_string(4)
        setValue = random_string(7)
        t = threading.Thread(target=(memcacheClientSet), args=(
            random.choice(server_addresses), setKey, setValue))
        threads.append(t)

    # for t in threads:
    #     t.start()

    # for t in threads:
    #     t.join()

    # threads = []

    for i in range(numRequests):
        getKey = '@GJ0_'
        t = threading.Thread(target=(memcacheClientGet), args=(
            random.choice(server_addresses), getKey,))
        threads.append(t)

    random.shuffle(threads)

    for t in threads:
        t.start()

    for t in threads:
        t.join()
