from distributed_sequencer import DistributedSequencer
import configparser
import socket
import concurrent.futures
import random
import threading
import time

mutex = threading.Lock()

if __name__ == "__main__":
    ds = DistributedSequencer(numOfServers = 10)

    config = configparser.ConfigParser()
    config.read('.env')

    servers = ds.getServers()

    result = []

    print("Available servers", servers)
    
    # Concurrent request to all the servers

    print("===============TEST 1: STARTING =================")
    print("A single client sending request to every server one by one.")
    # TEST 1: A single client sending request to every server one by one.
    for server in servers:
        sequencer_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sequencer_server.connect((server[0], server[1]))

        sequencer_server.send(b"get_id")

        BUFF_SIZE = 1024

        output = sequencer_server.recv(BUFF_SIZE)

        sequencer_server.close()

        output = int(output.decode('utf8'))
        
        print(output)
    print("===============TEST 1: COMPLETED =================")

    print("===============TEST 2: STARTING =================")
    print("A single client connects to the same server ")

    def test_two(sequencer_server):
        BUFF_SIZE = 1024

        start_time = time.time()
        
        output = sequencer_server.recv(BUFF_SIZE)

        sequencer_server.close()

        output = int(output.decode('utf8'))
        
        print(start_time, output)
    
    threads = []
    for _ in range(10):
        sequencer_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sequencer_server.connect((servers[0][0], servers[0][1]))

        sequencer_server.send(b"get_id")

        threads.append(threading.Thread(target=(test_two), args=(sequencer_server,)))

    for t in threads:
        t.start()
    
    for t in threads:
        t.join()

    print("===============TEST 2: COMPLETED =================")

    print("===============TEST 3: STARTING =================")
    print("Multiple client one server ")
    
    def connect_test(id, test_result):
        sequencer_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sequencer_server.connect((servers[id][0], servers[id][1]))

        mutex.acquire()

        sequencer_server.send(b"get_id")
        start_time = time.time()

        mutex.release()

        BUFF_SIZE = 1024

        output = sequencer_server.recv(BUFF_SIZE)

        sequencer_server.close()

        if output == '' or not output:
            print("----------------------------")
            return

        output = int(output.decode('utf8'))
        
        test_result.add((start_time, output))

    test_result = set()
    threads = []
    for _ in range(200):
        threads.append(threading.Thread(target=connect_test, args=(0,test_result,)))
    
    for t in threads:
        t.start()
    
    for t in threads:
        t.join()

    test_result = list(test_result)
    test_result = sorted(test_result, key = lambda x: x[1])

    print(test_result)

    print("number of results: ",len(test_result))
    
    print("===============TEST 3: COMPLETED =================")

    print("===============TEST 4: STARTING =================")
    print("Multiple clients connect to multiple servers concurrently")

    def connect_test(id, test_result):
        sequencer_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sequencer_server.connect((servers[id][0], servers[id][1]))

        mutex.acquire()

        sequencer_server.send(b"get_id")
        start_time = time.time()

        mutex.release()

        BUFF_SIZE = 1024

        output = sequencer_server.recv(BUFF_SIZE)

        sequencer_server.close()
        if output == '' or not output:
            print("----------------------------")
            return
        
        output = int(output.decode('utf8'))
        
        test_result.add((start_time, output))

    test_result = set()
    threads = []
    for _ in range(200):
        id = random.randrange(len(servers))
        threads.append(threading.Thread(target=connect_test, args=(id,test_result,)))
    
    for t in threads:
        t.start()
    
    for t in threads:
        t.join()

    test_result = list(test_result)
    test_result = sorted(test_result, key = lambda x: x[1])

    print(test_result)
    print("===============TEST 4: COMPLETED =================")
            
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     futures = []
        
    #     for i in range(1000):
    #         id = random.randrange(len(servers))
    #         futures.append(executor.submit(connect_test, id=id))

    #     for future in concurrent.futures.as_completed(futures):
    #         test_result.append(future.result())