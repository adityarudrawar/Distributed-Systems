from distributedkvstore import kvstore
from pymemcache.client.base import Client
import socket

# Eventual Local Read, writes to anyserver with asynchronous broadcast to every one else with a logical key respective to each key
# Linear Read/Write => TOB
# Sequential Local read, write is TOB [No master is required]


if __name__=="__main__":
    PAYLOAD_SIZE = 4096

    kvs = kvstore.KVStore( 
        consistency= 'linearizable_consistency',
        replicas = 3,
        storage_directory='C:\Work\Projects\Distributed_Systems\distributed_key_value\distributedkvstore\storage\key_value'
    )

    kvs.start()

    server_addresses = kvs.get_controlet_address()

    print(server_addresses)

    # Connect to a client
    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientSocket.connect(server_addresses[0]) 
    
    setKey = '@GJ0_'
    setValue = 'Value_1'
    no_reply = False
    setCommand = b'set' + b' ' + setKey.encode() + b' ' + setValue.encode() + b'\r\n'
    clientSocket.sendall(setCommand)
    response = clientSocket.recv(PAYLOAD_SIZE)

    print("Response for set key", response)
    clientSocket.close()

    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientSocket.connect(server_addresses[0]) 
    
    print('get key request')    
    getKey = '@GJ0_'
    getCommand = b'get' + b' ' + setKey.encode() + b'\r\n'
    clientSocket.sendall(getCommand)
    response = clientSocket.recv(PAYLOAD_SIZE)

    print("Response for get key", response)
    clientSocket.close()


