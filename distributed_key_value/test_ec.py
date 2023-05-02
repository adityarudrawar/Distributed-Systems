import threading
import string
import random
import time
import socket
from pymemcache.client.base import Client
from distributedkvstore import kvstore
import time
import matplotlib.pyplot as plt


EVENTUAL_CONSISTENCY = 'eventual_consistency'
LINEARIZABLE_CONSISTENCY = 'linearizable_consistency'
SEQUENTIAL_CONSISTENCY = 'sequential_consistency'
# NOT IMPLEMENTED CASUAL_CONSISTENCY = 'casual_consistency'


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
        consistency='eventual_consistency',
        replicas=5,
        storage_directory='C:\Work\Projects\Distributed_Systems\distributed_key_value\distributedkvstore\storage\key_value',
        output_directory='C:\Work\Projects\Distributed_Systems\distributed_key_value\distributedkvstore\output\eventual_consistency_output'
        )

    kvs.start()

    time.sleep(2)

    server_addresses = kvs.get_controlet_address()

    numRequests = 10

    # sends write request
    # Stale the broadcast to the other datalets by introducing time.sleep
    # Put a read request. 
    # Wait for the time.sleep to get over and then send a read request again.

    print("Setting the initial value of the key 'test_key' to 1")
    threading.Thread(target=(memcacheClientSet), args=(server_addresses[2], 'test_key', 1,)).start()

    time.sleep(10)

    print("Checking the value is updated everywhere")
    threads = []
    for address in (server_addresses):
        threads.append(threading.Thread(target=(memcacheClientGet), args=(address, 'test_key', )))
    
    for t in threads:
        t.start()
    
    for t in threads:
        t.join()

    print("Now starting a new write to set 'test_key' to 2")    
    threading.Thread(target=(memcacheClientSet), args=(server_addresses[2], 'test_key', 2,)).start()
    print("time.sleep introduces delay in the broadcast.")

    print("Showcasing the stale read, After a write operation it should be have been 2 in a linear mannaer, but it is 1.")
    t = threading.Thread(target=(memcacheClientGet), args=(server_addresses[4], 'test_key',))
    
    t.start()
    t.join()
    
    print("Waiting for the write to complete")
    time.sleep(7)

    print("Showcasing the updated value from the write should be 2 after enough time has passed.")
    threads = []
    for address in server_addresses:
        threads.append(threading.Thread(target=(memcacheClientGet), args=(address, 'test_key', )))

    for t in threads:
        t.start()
    
    for t in threads:
        t.join()

    print(f"test completed {numRequests}")