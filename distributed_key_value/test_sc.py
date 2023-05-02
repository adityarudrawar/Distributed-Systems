import threading
import string
import random
import time
import socket
from pymemcache.client.base import Client
from distributedkvstore import kvstore
import time
import matplotlib.pyplot as plt
import configparser

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

    config = configparser.ConfigParser()
    config.read('.env')

    SYSTEM_HOST = config["SEQUENTIAL"]["SYSTEM_HOST"]
    OUTPUT_DIRECTORY = config["SEQUENTIAL"]["OUTPUT_DIRECTORY"]
    STORAGE_DIRECTORY = config["SEQUENTIAL"]["SOTRAGE_DIRECTORY"]
    REPLICAS=int(config["SEQUENTIAL"]["REPLICAS"])
    CONSISTENCY = config["SEQUENTIAL"]["CONSISTENCY"]

    kvs = kvstore.KVStore(
        system_host=SYSTEM_HOST,
        consistency=CONSISTENCY,
        replicas=REPLICAS,
        storage_directory=STORAGE_DIRECTORY,
        output_directory=OUTPUT_DIRECTORY)


    kvs.start()

    time.sleep(2)
    
    server_addresses = kvs.get_controlet_address()

    numRequests = 5
    keys_generated = [random_string(7) + "_" + str(i) for i in range(numRequests)]


    # SET TEST CASE
    print("++++++++++ TEST 1: MULTIPLE SET OPERATIONS ON MULTIPLE SERVERS+++++++++++++++++++")
    threads = []
    for i in range(numRequests):
        setKey = keys_generated[i]
        setValue = random_string(7)
        t = threading.Thread(target=(memcacheClientSet), args=(
            random.choice(server_addresses), setKey, setValue,))
        threads.append(t)

    random.shuffle(threads)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print("SET TEST CASE COMPLETED")

    # GET TEST CASE
    print("++++++++++ TEST 2: MULTIPLE GET OPERATIONS ON MULTIPLE SERVERS+++++++++++++++++++")
    threads = []
    for i in range(numRequests):
        getKey = keys_generated[i]
        t = threading.Thread(target=(memcacheClientGet), args=(
            random.choice(server_addresses), getKey,))
        threads.append(t)

    random.shuffle(threads)

    for t in threads:
        t.start()

    for t in threads:
        t.join()


    print("GET TEST CASE COMPLETED")
    
    # SET AND GET TEST CASE
    print("++++++++++ TEST 3: MULTIPLE CONCURRENT SET AND GET  OPERATIONS ON MULTIPLE SERVERS+++++++++++++++++++")
    threads = []
    for i in range(numRequests):
        if i % 2 == 0:
            getKey = random.choice(keys_generated)
            t = threading.Thread(target=(memcacheClientGet), args=(
                random.choice(server_addresses), getKey,))
            threads.append(t)
        else:
            setKey = keys_generated[i]
            setValue = random_string(7)
            t = threading.Thread(target=(memcacheClientSet), args=(
                random.choice(server_addresses), setKey, setValue,))
            threads.append(t)
    
    random.shuffle(threads)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print(f"test completed {numRequests}")