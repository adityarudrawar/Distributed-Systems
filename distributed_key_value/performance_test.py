import threading
import string
import random
import time
import socket
from pymemcache.client.base import Client
from distributedkvstore import kvstore
import time
import matplotlib.pyplot as plt

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


def memcacheClientSet(address, key, value, response_times, index):
    start_time = time.time()
    c = Client(address)
    response = c.set(key, value, noreply=False)
    end_time = time.time()
    c.close()
    response_times[index] = end_time - start_time


def memcacheClientGet(address, key, response_times, index):
    start_time = time.time()
    c = Client(address)
    response = c.get(key)
    end_time = time.time()
    c.close()
    response_times[index] = end_time - start_time


if __name__ == "__main__":
    PAYLOAD_SIZE = 4096

    kvs = kvstore.KVStore(
        consistency='eventual_consistency',
        replicas=3,
        storage_directory='C:\Work\Projects\Distributed_Systems\distributed_key_value\distributedkvstore\storage\key_value',
        )

    kvs.start()

    server_addresses = kvs.get_controlet_address()

    set_avg_response_times = []
    get_avg_response_times = []
    set_and_get_avg_response_times = []
    
    num_requests_range = [100]
    for num in (num_requests_range):
        numRequests = num
        keys_generated = [random_string(7) + "_" + str(i) for i in range(numRequests)]


        # SET TEST CASE
        print(f" {num} SET TEST CASE STARTED")

        response_times = [float("inf") for _ in range(numRequests)]
        threads = []
        for i in range(numRequests):
            setKey = keys_generated[i]
            setValue = random_string(7)
            t = threading.Thread(target=(memcacheClientSet), args=(
                random.choice(server_addresses), setKey, setValue, response_times, i))
            threads.append(t)

        # random.shuffle(threads)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        avg_response_time = sum(response_times) / num
        set_avg_response_times.append(avg_response_time)
        print(f" {num} SET TEST CASE COMPLETED")

        # GET TEST CASE
        print(f" {num} GET TEST CASE STARTED")
        response_times = [float("inf") for _ in range(numRequests)]
        threads = []
        for i in range(numRequests):
            getKey = keys_generated[i]
            t = threading.Thread(target=(memcacheClientGet), args=(
                random.choice(server_addresses), getKey, response_times, i))
            threads.append(t)

        # random.shuffle(threads)

        for t in threads:
            t.start()

        for t in threads:
            t.join()
   
        avg_response_time = sum(response_times) / num
        get_avg_response_times.append(avg_response_time)

        print(f"{num} GET TEST CASE COMPLETED")
        
        # SET AND GET TEST CASE
        print(f" {num} GET, SET TEST CASE STARTED")
        response_times = [float("inf") for _ in range(numRequests)]
        threads = []
        for i in range(numRequests):
            if i % 2 == 0:
                getKey = random.choice(keys_generated)
                t = threading.Thread(target=(memcacheClientGet), args=(
                    random.choice(server_addresses), getKey, response_times, i))
                threads.append(t)
            else:
                setKey = keys_generated[i]
                setValue = random_string(7)
                t = threading.Thread(target=(memcacheClientSet), args=(
                    random.choice(server_addresses), setKey, setValue, response_times, i))
                threads.append(t)
        
        # random.shuffle(threads)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        avg_response_time = sum(response_times) / num
        set_and_get_avg_response_times.append(avg_response_time)

        print(f"{num} GET and TEST CASE COMPLETED")

    print(set_avg_response_times)
    print(get_avg_response_times)
    print(set_and_get_avg_response_times)

# Linear
# [9.6444002866745] SET
# [10.120154695510864] GET
# [9.773513970375062] SET AND GET


# Sequential
# [9.771539509296417] SET 
# [1.0758793091773986] GET
# [3.4528001666069033] SET AND GET


# Eventual
# [0.030848305225372314] SET
# [0.01655601501464844] GET
# [0.012655572891235351] SET AND GET