import socket
import os
from dotenv import load_dotenv
import time
import random
import string
import threading
import pdb
from pymemcache.client.base import Client

if __name__ == "__main__":
    try:
        print("client intialized")

        load_dotenv()

        CLIENT_HOST = os.getenv("CLIENT_HOST")
        CLIENT_PORT = int(os.getenv("CLIENT_PORT"))
        PAYLOAD_SIZE = int(os.getenv("PAYLOAD_SIZE"))


        def validChecks():
            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # clientSocket.settimeout(5)
            clientSocket.connect((CLIENT_HOST, CLIENT_PORT)) 
            
            setKey = '@GJ0_'
            setValue = 'Value_1'
            no_reply = False
            setCommand = b'set' + b' ' + setKey.encode() + b' ' + b'0' + b' ' + b'0' + b' ' + str(len(setValue)).encode() + (b'' if no_reply == False else b' noreply') + b'\r\n' + setValue.encode() + b'\r\n'
            clientSocket.sendall(setCommand)
            response = clientSocket.recv(PAYLOAD_SIZE)

            print("Response for Invalid key", response)

            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # clientSocket.settimeout(5)
            clientSocket.connect((CLIENT_HOST, CLIENT_PORT)) 
            
            clientSocket.sendall(b'carry 10\r\n')
            response = clientSocket.recv(PAYLOAD_SIZE)

            print("Response for Invalid command", response)

            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # clientSocket.settimeout(5)
            clientSocket.connect((CLIENT_HOST, CLIENT_PORT)) 
            
            setKey = 'Key_1'
            setValue = 'Value_1'
            no_reply = False
            setCommand = b'set' + b' ' + setKey.encode() + b' ' + b'0' + b' ' + b'0' + b' ' + str(len(setValue) + 2).encode() + (b'' if no_reply == False else b' noreply') + b'\r\n' + setValue.encode() + b'\r\n'
            clientSocket.sendall(setCommand)
            response = clientSocket.recv(PAYLOAD_SIZE)

            print("Response for Invalid length of value", response)

            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # clientSocket.settimeout(5)
            clientSocket.connect((CLIENT_HOST, CLIENT_PORT)) 
            
            key = 'Key1'
            getCommand = b'get' + b' ' + key.encode() + b'\r\n'    
            clientSocket.sendall(getCommand)
            response = clientSocket.recv(PAYLOAD_SIZE)
        
            print("Response for key not present in KV Store", response)
                
        validChecks()

        print("TEST CASES START")

        threads = []
        generatedKeys = []
        
        setResultsPerClient = []
        getResultsPerClient = []
        numberOfClients = 3

        client = None

        def setFunction(id, flag):
            try:
                if flag:
                    setKey = ''.join(random.choices(string.ascii_uppercase, k=10))
                    generatedKeys.append(setKey)
                else:
                    setKey = generatedKeys[id]

                setValue = ''.join(random.choices(string.ascii_lowercase, k=50))
                
                no_reply = random.choice([False, True])
                
                start = time.time()
                
                clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                clientSocket.settimeout(5)
                clientSocket.connect((CLIENT_HOST, CLIENT_PORT))    

                # b'set ETWSKFYBAK 0 0 50\r\nxfvliagdmdtsqmurukihpzspfmsyofbrpzydpjjyumgamohhoy\r\n'
                # b'set UZARBDKKZN 0 0 50 noreply\r\nemonytakkywvnllgfojyivnqnikwgliydjunwgsjdimuxveywb\r\n'

                setCommand = b'set' + b' ' + setKey.encode() + b' ' + b'0' + b' ' + b'0' + b' ' + str(len(setValue)).encode() + (b'' if no_reply == False else b' noreply') + b'\r\n' + setValue.encode() + b'\r\n'

                clientSocket.sendall(setCommand)
                response = clientSocket.recv(PAYLOAD_SIZE)
                if response == b'STORED\r\n':
                    result = True
                else:
                    result = False
                
                end = time.time()

                setResultsPerClient.append([setKey,  result, end - start])

                print(f'SET Key: {setKey} Value: {setValue} ', 'STORED\r\n' if result else 'NOT_STORED\r\n')
                clientSocket.close()
            except Exception as e:
                print(e)    

        def getFunction(key):
            try:
                start = time.time()
                
                clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                clientSocket.settimeout(5)
                clientSocket.connect((CLIENT_HOST, CLIENT_PORT))    

                getCommand = b'get' + b' ' + key.encode() + b'\r\n'
                
                clientSocket.sendall(getCommand)
                response = clientSocket.recv(PAYLOAD_SIZE)
                
                if response != b"END\r\n":
                    response = response.decode()

                    response = response.split(" ")

                    value = b''
                    receivedData = clientSocket.recv(PAYLOAD_SIZE)

                    while receivedData != b"END\r\n":
                        value += receivedData
                        receivedData = clientSocket.recv(PAYLOAD_SIZE)
                
                    result = value
                else:
                    result = None
                    
                end = time.time()

                getResultsPerClient.append([key,  result, end - start])
                
                print(f'GET Key: {key} Value: {result}' )
                clientSocket.close()
            except Exception as e:
                print(e)
                
        # Concurrent SET requests
        for i in range(numberOfClients):
            t = threading.Thread(target=setFunction, args=(i, True, ))
            threads.append(t)
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
        
        print("-------------------------------------")
        print('generatedKeys')
        print(generatedKeys)

        
        print("TEST CASE 1: SET REQUESTS")
        print("-------------------------------------")
        print('setResultsPerClient')
        print(setResultsPerClient)

        threads = []

        # Concurrent GET requests
        for i in range(numberOfClients):
            t = threading.Thread(target=getFunction, args=(generatedKeys[i], ))
            threads.append(t)

        for t in threads:
            t.start()
        
        for t in threads:
            t.join()

        print("TEST CASE 2: GET REQUESTS")
        print("-------------------------------------")
        print('getResultsPerClient')
        print(getResultsPerClient)

        threads = []
        setResultsPerClient = []
        getResultsPerClient = []

        for i in range(numberOfClients):
            t1 = threading.Thread(target=setFunction, args=(i, False, ))
            threads.append(t1)

            t2 = threading.Thread(target=getFunction, args=(generatedKeys[i], ))
            threads.append(t2)

        random.shuffle(threads)

        for t in threads:
            t.start()
        
        for t in threads:
            t.join()

        print("TEST CASE 3: SET AND GET REQUESTS")
        print("-------------------------------------")
        print('setResultsPerClient')
        print(setResultsPerClient)

        print("-------------------------------------")
        print('getResultsPerClient')
        print(getResultsPerClient)

        threads = []
        getResultsPerClient = []

        randomKeys = [''.join(random.choices(string.ascii_uppercase, k=10)) for _ in range(numberOfClients)]

        for i in range(numberOfClients):
            t = threading.Thread(target=getFunction, args=(randomKeys[i],))
            threads.append(t)
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()

        print("TEST CASE 4: RANDOM KEY REQUESTS")
        print("-------------------------------------")
        print('getResultsPerClient')
        print(getResultsPerClient)

        
    except Exception as e:
        print(e)