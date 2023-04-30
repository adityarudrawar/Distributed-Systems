import threading
import string
import random
import time
import socket
from pymemcache.client.base import Client
from distributedkvstore import kvstore

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

# Remove the default params
def memcacheClientSet(address, key, value, id = 0, noreply=False, expire=0,flags = -1):
    c = Client(address,default_noreply=False)
    response = c.set(key, value)
    print(f"SET {id} response ", response)
    c.close()


def memcacheClientGet(address, key, id = 0):
    c = Client(address)
    response = c.get(key)
    print(f"GET {id} response {response}")
    c.close()


if __name__ == "__main__":
    PAYLOAD_SIZE = 4096

    kvs = kvstore.KVStore(
        consistency='eventual_consistency',
        replicas= 10,
        storage_directory='C:\Work\Projects\Distributed_Systems\distributed_key_value\distributedkvstore\storage\key_value',
        output_directory='C:\Work\Projects\Distributed_Systems\distributed_key_value\distributedkvstore\output\eventual_consistency_output'
        )

    kvs.start()

    server_addresses = kvs.get_controlet_address()

    numRequests = 100

    ################################################# SET REQUEST #######################################
    threads = []
    for i in range(numRequests):
        setKey = 'test_key'
        setValue = random_string(7)
        threads.append(threading.Thread(target=(memcacheClientSet), args=(random.choice(server_addresses), setKey, setValue, i)))

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    time.sleep(3)
    print("################################################################# GET REQUEST ####################")
    threads = []

    threads.append(threading.Thread(target=(memcacheClientGet), args=(server_addresses[0], 'test_key', 0)))
    threads.append(threading.Thread(target=(memcacheClientGet), args=(server_addresses[1], 'test_key', 1)))
    threads.append(threading.Thread(target=(memcacheClientGet), args=(server_addresses[2], 'test_key', 2)))
    threads.append(threading.Thread(target=(memcacheClientGet), args=(server_addresses[3], 'test_key', 3)))
    threads.append(threading.Thread(target=(memcacheClientGet), args=(server_addresses[4], 'test_key', 4)))

    threads.append(threading.Thread(target=(memcacheClientGet), args=(server_addresses[5], 'test_key', 5)))
    threads.append(threading.Thread(target=(memcacheClientGet), args=(server_addresses[6], 'test_key', 6)))
    threads.append(threading.Thread(target=(memcacheClientGet), args=(server_addresses[7], 'test_key', 7)))
    threads.append(threading.Thread(target=(memcacheClientGet), args=(server_addresses[8], 'test_key', 8)))
    threads.append(threading.Thread(target=(memcacheClientGet), args=(server_addresses[9], 'test_key', 9)))

    for t in threads:
        t.start()

    for t in threads:
        t.join()