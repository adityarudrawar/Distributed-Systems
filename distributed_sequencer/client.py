from distributed_sequencer import DistributedSequencer
import configparser
import socket
import concurrent.futures
import random

if __name__ == "__main__":
    ds = DistributedSequencer(numOfServers = 10)

    config = configparser.ConfigParser()
    config.read('.env')

    servers = ds.getServers()

    result = []

    print("Available servers", servers)
    
    def connect_test(id):
        sequencer_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sequencer_server.connect((servers[0][0], servers[0][1]))

        sequencer_server.send(b"get_id")

        BUFF_SIZE = 1024

        output = sequencer_server.recv(BUFF_SIZE)

        sequencer_server.close()

        output = int(output.decode('utf8'))
        
        return output
    
    test_result = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        
        for i in range(1000):
            id = random.randrange(len(servers))
            futures.append(executor.submit(connect_test, id=id))

        for future in concurrent.futures.as_completed(futures):
            test_result.append(future.result())
    
    if len(test_result) == len(set(test_result)):
        print("No duplicates")
    else:
        print("Duplicates")