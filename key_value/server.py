import socket
import os
from dotenv import load_dotenv
import threading
import time
import json
import pdb
import datetime
import time
import os

def keyIsValid(key):
    pass

def writeToFile(key, object):
    try:
        while global_lock.locked():
            time.sleep(0.01)
            continue

        global_lock.acquire()

        dataStore[key] = object

        fp = open('data.json', 'w')
        
        json.dump(dataStore, fp)
        
        fp.close()

        global_lock.release()

        return True
    except Exception as e:
        if fp:
            fp.close()

        global_lock.release()

        print(f"Exception raised while writing to file: {e}")

        return False

def readFromFile(key):
    try:

        while global_lock.locked():
            time.sleep(0.01)
            continue

        fp = open('data.json', 'r')
        
        dataStore = json.load(fp)
        
        if key not in dataStore:
            return None

        obj = dataStore[key]
        
        # TODO: Logic for expiry
        fp.close()

        return obj
    
    except Exception as e:
        print(f"Exception raised while reading file for key: {key} =>: {e}")
        # pdb.set_trace()
        return None

def recvAll(conn, valueSize):
    
    data = b''
    
    while True:

        chunk = conn.recv(PAYLOAD_SIZE)
        data += chunk
        if len(data) >= valueSize: 
            # When the total data received becomes greater than the valueSize 
            # indicates this was the last chunk as it had extra size of 5 of ' \r\n'
            break
    
    return data.decode()

def setFunction(request, conn, id):

    try:
        tokenPos = request.find('\r\n')
        
        before, after = request[:tokenPos], request[tokenPos + 2 :]
        before = before.split(" ")

        key = before[1]
        flags = before[2]
        expiry = before[3]
        
        if len(before) <= 5:
            noReply = False
            valueSize = int(before[4].split("\r\n")[0])
            value = after[:valueSize]
        else:
            noReply = True
            valueSize = int(before[4])
            value = after[:valueSize]
        
        if noReply:
            conn.sendall(b"STORED\r\n")
    
        print("setFunction =>",key, flags, expiry, valueSize, noReply, value)
        # [0 => value, 1 => valueSize, 2 => flags, 3 => expiry, 4 => noReply, 6 => created_at]
        object = [
            value,
            valueSize,
            flags,
            expiry,
            noReply
        ]

        response = b''

        if writeToFile(key, object):
            response = b"STORED\r\n"
        else:
            response = b"NOT_STORED\r\n"
        
        if not noReply:
            conn.sendall(response)
    except Exception as e:
        print("Exception raised in setFunction")
        print(e)
        conn.sendall(b"NOT_STORED\r\n")

def getFunction(request, conn, id):
    try:
        key = request.split(' ')[1].replace('\r\n', '')

        object = readFromFile(key)

         # [0 => value, 1 => valueSize, 2 => flags, 3 => expiry, 4 => noReply, 6 => created_at]

        if object != None:
            # VALUE flags valueSize\r\n
            # value\r\n
            cmd = b'VALUE' + b' ' + key.encode() + b' ' + str(object[2]).encode() + b' ' + str(object[1]).encode() + b'\r\n'
            conn.sendall(cmd)
            conn.sendall(object[0].encode() + b'\r\n' )
        
        conn.sendall(b"END\r\n")
        print(f"Get request finished for client id: {id}")
    except Exception as e:
        print(f"Exception raised in getFunction for client id: {id}")
        print(e)

if __name__ == "__main__":

    print("server initialzed")

    load_dotenv()
    global_lock = threading.Lock()

    SERVER_HOST = os.getenv("SERVER_HOST")
    SERVER_PORT = int(os.getenv("SERVER_PORT"))
    PAYLOAD_SIZE = int(os.getenv("PAYLOAD_SIZE"))
    
    # If data.json does not exists or if it exists but is empty
    if not os.path.isfile('data.json') or os.stat("data.json").st_size == 0:
        fp = open('data.json', 'w')
        json.dump({}, fp)
        dataStore = {}
        fp.close()
    else:
        fp = open('data.json', 'r')        
        dataStore = json.load(fp)
        fp.close()

    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    serverSocket.bind((SERVER_HOST, SERVER_PORT))

    serverSocket.listen()
    i = 0

    while True:
        conn, addr = serverSocket.accept()
        try:
            request = conn.recv(PAYLOAD_SIZE)
            
            request = request.decode()

            if request[:3] == "set":
                print("setting new value")
                threading.Thread(target=setFunction, args=(request, conn, id, )).start()
            elif request[:3] == "get":
                print("getting new value")
                threading.Thread(target=getFunction, args=(request, conn, id, )).start()
            else:
                print("Invalid command")
        except Exception as e:
            print(f"Exception raised while connecting to client id: {id}")
            print(e)

        print("new client connected: ",i)
        
        i += 1
        
    serverSocket.shutdown(socket.SHUT_RDWR)