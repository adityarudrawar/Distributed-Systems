from multiprocessing import Process
from pymemcache.client.base import Client
import socket
import threading
import heapq


class Request:
    def __init__(self):
        self.clock: float
        self.message = ''


class Controlet(Process):
    def __init__(self, address, id, datalets):
        super(Controlet, self).__init__()

        self.address = address

        self.counter = 0

        self.id = id

        self.node_datalet = datalets[id]

        self.queue = []
        self.sorting_lambda = lambda x: (x[0], x[1])

        self.other_datalets = datalets[:id] + datalets[id + 1:]

        print("clock: ", self.clock)

    def __getClock(self):
        return float(str(self.counter) + "." + str(self.id))

    def __broadcastWrite(self, key, value):
        for i in range(self.other_datalets):
            c = Client(self.other_datalets[i])
            c.set(key, value)
            c.close()

    def __addToQueue(self, ele):
        heapq.heappush()

    def __setKey(self, conn, key, value):
        c = Client(self.node_datalet)
        response = c.set(key, value)
        conn.sendall(str(response).encode('utf8'))

        self.__broadcastWrite(key, value)
        c.close()

    def __getKey(self, conn, key):
        c = Client(self.node_datalet)
        reponse = c.get(key)
        conn.sendall(reponse)
        c.close()

    def run(self):
        print("Controlet process started")

        # Start the listening process.
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        serverSocket.bind(self.address)

        serverSocket.listen()
        id = 0

        while True:
            try:
                conn, addr = serverSocket.accept()

                request = conn.recv(4096)

                request = request.decode('utf8')

                if request[:3] == "set":
                    split_resp = request.split(" ")
                    print("set key: ", split_resp[1])
                    print("set value: ", split_resp[2].split('\r\n')[0])

                    threading.Thread(target=self.__setKey, args=(
                        conn, split_resp[1], split_resp[2].split('\r\n')[0],)).start()
                elif request[:3] == "get":
                    split_resp = request.split(" ")
                    print("get key: ", split_resp[1])
                    threading.Thread(target=self.__getKey, args=(
                        conn, split_resp[1].split('\r\n')[0],)).start()
                else:
                    conn.sendall(b"INVALID_COMMAND\r\n")
            except Exception as e:
                print(f"Exception raised in: {self.clock}")
                print(e)

            id += 1
