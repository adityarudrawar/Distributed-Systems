from multiprocessing import Process
import socket
import threading
from threading import Lock

mutex = Lock()

class SequencerServer(Process):
    def __init__(self, host, port, id, primaryServer):
        super(SequencerServer, self).__init__()
        self.host = host
        self.port = port

        self.id = id

        self.state = 0

        self.__primaryServerBindings = primaryServer
        self.__primaryServerId = 0

    def __incrementState(self):
        mutex.acquire()

        try:
            self.state += 1
        except Exception as e:
            pass
        finally:
            mutex.release()
    
    def __processGetIdRequest(self, conn):
        try:
            BUFF_SIZE = 1024
            
            # connect to the primary server
            p_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            p_server_socket.connect(self.__primaryServerBindings)

            message = b"get_id " + str(self.id).encode('utf8')
            
            # Request to the primary server
            p_server_socket.send(message)
            
            response = p_server_socket.recv(BUFF_SIZE)

            response = response.decode('utf8')

            if "id" not in response:
                raise Exception

            uid = int(response.split(" ")[1])
            message = b"" + str(uid).encode('utf8')

            conn.send(message)            

        except Exception as e:
            print(f"Sequencer server {self.id} :: __processGetIdRequest::", e)
    
    def __startConnection(self):
        
        BUFF_SIZE = 1024

        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        serverSocket.bind((self.host, self.port))

        serverSocket.listen()

        while True:
            try:
                conn, addr = serverSocket.accept()

                request = conn.recv(BUFF_SIZE)

                request = request.decode('utf8')

                # Increment the time every time you receive a message
                if "get_id" in request:
                    threading.Thread(target=(self.__processGetIdRequest), args=(conn,)).start()
                elif "id" in request:
                    # Update the id by acquiring a mutex lock
                    id = int(request.split(" ")[1])
                    self.state = id
                else:
                    conn.send(b"Invalid request\r\n")
            except Exception as e:
                print(f"Sequencer server {self.id} :: __startConnection:: ", e)

    def run(self):
        # Start the connection
        # Create a thread for every connection 
        self.__startConnection()