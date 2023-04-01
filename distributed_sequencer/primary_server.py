from multiprocessing import Process
import socket
import threading
from threading import Lock

mutex = Lock()

class PrimaryServer(Process):
    def __init__(self, host, port, id, sequencer_servers):
        super(PrimaryServer, self).__init__()
        self.host = host
        self.port = port
        self.id = id

        self.sequencer_servers = sequencer_servers

        self.state = 0
    
    def __incrementState(self):
        mutex.acquire()

        try:
            self.state += 1
        except Exception as e:
            pass
        finally:
            mutex.release()

    def __processGetIdRequest(self, server_conn, server_id):
        # Get a mutex lock
        # Increment the counter
        # Broadcast the state to the conn and all the other conenction
        # Do I need to wait for acknowledgment

        self.__incrementState()

        message = b"id " + str(self.state).encode('utf8')

        try:
            # Broadcast to all the other servers
            for host, port, id in self.sequencer_servers:
                if id == server_id:
                    server_conn.send(message)

                    server_conn.close()
                else:
                    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    server_socket.connect((host, port))

                    server_socket.send(message)

                    # Do we need to close the socket or not?
                    server_socket.close()

        except Exception as e:
            print("Primary server:: __processGetIdRequest::", e)


    # Change this function name to run 
    def run(self):
        
        BUFF_SIZE = 1024

        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        serverSocket.bind((self.host, self.port))

        serverSocket.listen()

        while True:
            try:
                conn, addr = serverSocket.accept() 

                request = conn.recv(BUFF_SIZE)

                request = request.decode('utf8')

                if 'get_id' in request:
                    server_id = int(request.split(" ")[1])
                    threading.Thread(target=(self.__processGetIdRequest), args=(conn, server_id)).start()

            except Exception as e:
                print("Primary server:: run::", e)