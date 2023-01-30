import socket
import os
from dotenv import load_dotenv
import threading
import time
import json
import pdb

d = {}

def writeToFile(key, object):
    try:
        while global_lock.locked():
            time.sleep(0.01)
            continue

        global_lock.acquire()

        d[key] = object

        fp = open('data.json', 'w')
        
        json.dump(d, fp)
        
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

        fp = open('data.json', 'r')
        
        d = json.load(fp)
        
        if key not in d:
            return None

        obj = d[key]

        fp.close()

        return obj
    
    except Exception as e:
        print(f"Exception raised while reading file for key: {key} =>: {e}")
    
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

def setFunction(request, conn):

    # noreply : True =>  ['set', 'some_key', '0', '0', '10', 'noreply\r\nsome_value\r\n']
    # noreply : False => ['set', 'some_key', '0', '0', '10\r\nsome_value\r\n']
    try:
        # User this logic for split base.py : 1649
        # .find(b"\r\n")
        # tokenPos = request.find('\r\n')
        request = request.split(" ")
        key = request[1]
        flags = request[2]
        expiry = request[3]

        if len(request) <= 5:
            noReply = False
            valueSize = int(request[4].split("\r\n")[0])
            value = request[4].split("\r\n")[1]
        else:
            noReply = True
            valueSize = int(request[4])
            value = request[5].split("\r\n")[1]

        print(key, flags, expiry, noReply, valueSize, value)

        object = {
            "value": value,
            "valueSize": valueSize,
            "flags": flags,
            "expiry": expiry,
            "noReply": noReply,
        }
        response = b''

        if writeToFile(key, object):
            response = b"STORED\r\n"
        else:
            response = b"NOT_STORED\r\n"
        
        conn.sendall(response)
    except Exception as e:
        print(e)

def getFunction(request, conn):

    key = request.split(' ')[1].replace('\r\n', '')

    object = readFromFile(key)

    if object != None:
        cmd = b'VALUE' + b' ' + key.encode() + b' ' + str(object['flags']).encode() + b' ' + str(object['valueSize']).encode() + b'\r\n'
        conn.sendall(cmd)
        conn.sendall(object['value'].encode() + b'\r\n' )
    
    conn.sendall("END\r\n".encode())

def getOrSet(request, conn):
    try:
        request = request.decode()

        if request[:3] == "set":
            print("setting new value")
            threading.Thread(target=setFunction, args=(request, conn, )).start()
        elif request[:3] == "get":
            print("getting new value")
            threading.Thread(target=getFunction, args=(request, conn, )).start()
        else:
            conn.sendall(b"non-acceptable command\r\n")
    except Exception as e:
        # pass
        print(e)

def handleClient(conn, addr, id):
        while True:
            try:
                request = conn.recv(PAYLOAD_SIZE)
                threading.Thread(target=getOrSet, args=(request, conn, )).start()
            except Exception as e:
                pass

                #  print(e)
        conn.close()
        print("connection closed: ", id)

if __name__ == "__main__":

    print("server initialzed")

    load_dotenv()
    global_lock = threading.Lock()

    SERVER_HOST = os.getenv("SERVER_HOST")
    SERVER_PORT = int(os.getenv("SERVER_PORT"))
    PAYLOAD_SIZE = int(os.getenv("PAYLOAD_SIZE"))

    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    serverSocket.bind((SERVER_HOST, SERVER_PORT))

    serverSocket.listen()
    i = 0

    while True:
        conn, addr = serverSocket.accept()

        t = threading.Thread(target=handleClient, args=(conn, addr, i, ))

        t.start()

        print("new client connected: ",i)
        
        i += 1
        
    serverSocket.shutdown(socket.SHUT_RDWR)