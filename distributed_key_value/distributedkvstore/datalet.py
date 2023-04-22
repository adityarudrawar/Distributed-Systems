import socket
import os
import threading
import json
import os
from multiprocessing import Process

class Datalet(Process):
    def __init__(self, address, storage_directory, id):
        super(Datalet, self).__init__() 

        self.address = address

        self.storage_directory = storage_directory + "_" +str(id)

        self.id = id
        
        print("Datalet intialized")
        if not os.path.exists(self.storage_directory):
            os.makedirs(self.storage_directory)
    
    def writeToFile(self, key, object):
        try:

            with open(self.storage_directory + "\\" + key, 'w+', encoding='utf8') as f:
                f.write(json.dumps({"value": object}, ensure_ascii=False))
                f.close()

            return True
        except Exception as e:
            print(f"Exception raised while writing to file: {e}")
            return False

    def readFromFile(self, key):
        try:
            if not os.path.exists(self.storage_directory + "\\" + key):
                return None
            
            f = open(self.storage_directory + "\\" + key, 'r', encoding='utf8')
            obj = json.load(f)
            f.close()
            return obj["value"]
            
        except Exception as e:
            print(e)
            print(f"Exception raised while reading file for key: {key} =>: {e}")
            return None


    def setFunction(self, request, conn, id):

        try:
            tokenPos = request.find('\r\n')
            
            commandList = request.split('\r\n')
           
            before = commandList[0]
            after = commandList[1]
            
            before = before.split(" ")

            key = before[1]
            flags = before[2]
            expiry = before[3]
            
            

            if len(before) <= 5:
                noReply = False
                valueSize = int(before[4].split("\r\n")[0])
                value = after
            else:
                noReply = True
                valueSize = int(before[4])
                value = after
            
            if noReply:
                conn.sendall(b"STORED\r\n")
        
            # [0 => value, 1 => valueSize, 2 => flags, 3 => expiry, 4 => noReply, 6 => created_at]
            object = [
                value,
                valueSize,
                flags,
                expiry,
                noReply
            ]

            response = b''

            # if writeToFile(key, object):
            if self.writeToFile(key, value):
                response = b"STORED\r\n"
            else:
                response = b"NOT_STORED\r\n"
            
            if not noReply:
                conn.sendall(response)
        except Exception as e:
            print("Exception raised in setFunction")
            print(e)
            conn.sendall(b"NOT_STORED\r\n")

    def getFunction(self, request, conn, id):
        try:
            key = request.split(' ')[1].replace('\r\n', '')

            object = self.readFromFile(key)

            # [0 => value, 1 => valueSize, 2 => flags, 3 => expiry, 4 => noReply, 6 => created_at]
            if object != None:
                # VALUE flags valueSize\r\n
                # value\r\n
                
                cmd = 'VALUE' + ' ' + key + ' ' + str(0) + ' ' + str(len(object.encode('utf8'))) + '\r\n'
                conn.send(cmd.encode('utf8'))
                conn.send( "{}\r\n".format(object).encode('utf8')  )
            
            conn.send("END\r\n".encode('utf8'))
        except Exception as e:
            print(f"Exception raised in getFunction for client id: {id}")
            print(e)

    def recvAll(self, conn):
        BUFF_SIZE = 1024
        data = b''    
        while True:
            part = conn.recv(BUFF_SIZE)
            data += part
            if len(part) < BUFF_SIZE:
                # either 0 or end of data
                break
        return data


    def run(self):
        print("Controlet process started")

        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        serverSocket.bind(self.address)

        serverSocket.listen()
        id = 0

        while True:
            try:
                conn, addr = serverSocket.accept()

                request = self.recvAll(conn)
                
                request = request.decode('utf8')

                if request[:3] == "set":
                    threading.Thread(target=self.setFunction, args=(request, conn, id,)).start()
                elif request[:3] == "get":
                    threading.Thread(target=self.getFunction, args=(request, conn, id,)).start()
                else:
                    conn.sendall(b"INVALID_COMMAND\r\n")
            except Exception as e:
                print(f"Exception raised while connecting to client id: {id}")
                print(e)
            
            id += 1