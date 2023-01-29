import socket
import os
from dotenv import load_dotenv
import threading
import time
import json
import pdb

d = {}

def writeToFile(key, value):
    try:
        while global_lock.locked():
            time.sleep(0.01)
            continue

        global_lock.acquire()

        d[key] = value

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
        value = ""

        fp = open('data.json', 'r')
        
        d = json.load(fp)
        
        if key not in d:
            value = ""
        value = d[key]


        fp.close()

        return value
    
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

    key = request[1]
    valueSize = int(request[2])

    value = recvAll(conn, valueSize)

    # value = conn.recv(PAYLOAD_SIZE).decode()

    if writeToFile(key, value):
        response = "STORED\r\n"
    else:
        response = "NOT-STORED\r\n"
    
    conn.sendall(response.encode())

def getFunction(request, conn):
     
    key = request[1].replace('\r\n', '')

    dataBlock = readFromFile(key)
    
    if dataBlock != None:
        conn.sendall(f"VALUE {key} {len(dataBlock)} \r\n".encode())
        time.sleep(1)
        conn.sendall( dataBlock.encode() )
        time.sleep(1)
    
    conn.sendall("END\r\n".encode())


def handleClient(conn, addr, id):
        
        while True:
            request = conn.recv(PAYLOAD_SIZE)

            request = request.decode()

            request = request.split(" ")

            if request[0] == "set":
                print("setting new value")
                setThread = threading.Thread(target=setFunction, args=(request, conn, ))
                setThread.start()
                setThread.join()

            elif request[0] == "get":
                print("getting new value")
                getThread = threading.Thread(target=getFunction, args=(request, conn, ))
                getThread.start()
                getThread.join()

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