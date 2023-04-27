from multiprocessing import Process, Queue
from pymemcache.client.base import Client
import socket
import threading
import os
import base64
import time

STATUS_KEY = 'status'
KEY_KEY = 'key'
VALUE_KEY = 'value'
NUM_ACK_KEY = 'ack'
REQ_FROM_KEY = 'req_from'

ACK_SENT = 'ACK_SENT'
ACK_RECV = 'ACK_RECV'

READY = 'READY'
SENT = 'SENT'
RECV = 'RECV'

REQ_TYPE = 'req_type'

PAYLOAD_SIZE = 4096

QUEUE_LOCK = threading.Lock()
COUNTER_LOCK = threading.Lock()


class Controlet(Process):
    def __init__(self, address, id, datalets, controlets):
        super(Controlet, self).__init__()

        self.address = address

        self.counter = 0

        self.id = id

        self.node_datalet = datalets[id]

        self.queue = []
        # self.sorting_lambda = lambda x: (x[0][0], x[0][1])

        self.other_datalets = datalets[:id] + datalets[id + 1:]
        self.__controlets = controlets

        self._map = {}

        self.__order = []

    def __getUniqueId(self):
        return base64.b64encode(os.urandom(6)).decode('ascii')

    def __incrementCounter(self):
        COUNTER_LOCK.acquire()
        self.counter += 1
        COUNTER_LOCK.release()

    def __getClock(self):
        return float(str(self.counter) + "." + str(self.id))

    def __appendToQueue(self, contents):

        QUEUE_LOCK.acquire()

        self.queue.append(contents)
        self.queue.sort(key=lambda x: (x[0][0], x[0][1]))

        QUEUE_LOCK.release()

    def __popFromQueue(self):
        QUEUE_LOCK.acquire()

        self.queue.pop(0)
        self.queue.sort(key=lambda x: (x[0][0], x[0][1]))

        QUEUE_LOCK.release()

    def __sendMessage(self, address, message):
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect(address)

        clientSocket.sendall(message)

        # Do we need this?
        clientSocket.recv(PAYLOAD_SIZE)

    def __broadcastMessage(self, message):

        for controlet in self.__controlets:
            if controlet == self.address:
                continue
            self.__sendMessage(controlet, message)

    def __handleClientSet(self, conn, key, value):
        self.__incrementCounter()
        logical_clock = str(self.__getClock())

        req_id = self.__getUniqueId()

        self._map[req_id] = {STATUS_KEY: SENT,
                             KEY_KEY: key, VALUE_KEY: value, NUM_ACK_KEY: 0, 'conn': conn, REQ_TYPE: 'set', REQ_FROM_KEY: 'client'}

        # Add to queue : [clock, req_id]
        self.__appendToQueue(
            [[int(logical_clock.split(".")[0]), int(logical_clock.split(".")[1])], req_id])

        message = self.__createMessage(
            ['req', req_id, logical_clock, 'set', key, value])

        # Broadcast to other controlets
        self.__broadcastMessage(message)

    def __handleClientGet(self, conn, key):
        # Increment clock
        # Generate a request id
        # Add the request to queue with the conn and the request id.
        # Broadcast the request

        # A seperate thread for queue operations?
        req_id = self.__getUniqueId()

        self.__incrementCounter()

        self._map[req_id] = {STATUS_KEY: SENT,
                             KEY_KEY: key, NUM_ACK_KEY: 0, 'conn': conn, REQ_TYPE: 'get', REQ_FROM_KEY: 'client'}

        logical_clock = str(self.__getClock())

        self.__appendToQueue(
            [[int(logical_clock.split(".")[0]), int(logical_clock.split(".")[1])], req_id])

        # TODO: better the key value size split that is not with a " ". This will cause a lot of errors in the future

        # Broadcast to other controlets
        message = self.__createMessage(
            ['req', req_id, logical_clock, 'get', key])

        self.__broadcastMessage(message)

        # Broadcast to all other servers

    def __setKey(self, key, value, conn=None):
        '''
        Sends the key value to the datalet
        '''
        c = Client(self.node_datalet)
        response = c.set(key, value)
        if conn != None:
            conn.sendall(str(response).encode('utf8'))
        c.close()

        return response

    def __getKey(self, key, conn=None):
        '''
        Gets the value from the datalet
        '''
        c = Client(self.node_datalet)
        response = c.get(key)
        if conn != None:
            conn.sendall(response)
        c.close()

        return response

    def __createMessage(self, params):
        if len(params) < 4:
            print("Raise exception cause params is less than 4")
            raise Exception

        message = b''
        for i in range(len(params)):
            par = params[i]
            message += str(par).encode('utf8')

            if i == len(params) - 1:
                continue
            message += b' '

        return message

    def __handleReq(self, request):
        # Req, req_id, timestamp, msg, ...
        split_req = request.split(" ")
        req, req_id, logical_clock, msg = split_req[0], split_req[1], \
            split_req[2], split_req[3]

        if msg == "set":
            key = split_req[4]
            value = split_req[5]
            if req_id in self._map:
                print(
                    f"Exception in _handleReq {self.id} msg: {msg} message already exists")

            self._map[req_id] = {STATUS_KEY: RECV,
                                 KEY_KEY: key, VALUE_KEY: value, REQ_TYPE: msg, REQ_FROM_KEY: 'server'}

            # Add to queue : [clock, msg, key, value] Can make this better? by giving the id in the list and while processing
            self.__appendToQueue(
                [[int(logical_clock.split(".")[0]), int(logical_clock.split(".")[1])], req_id])

            # Ack is sent when the msg is at the head of the queue
        elif msg == "get":
            key = split_req[4]

            if req_id in self._map:
                print(
                    f"Exception in _handleReq {self.id} msg: {msg} message already exists")

            self._map[req_id] = {STATUS_KEY: RECV,
                                 KEY_KEY: key, REQ_TYPE: msg, REQ_FROM_KEY: 'server'}

            # Add to queue : [clock, msg, key, value] Can make this better? by giving the id in the list and while processing
            self.__appendToQueue(
                [[int(logical_clock.split(".")[0]), int(logical_clock.split(".")[1])], req_id])

        # For the original sender
        elif msg == "ack":
            self._map[req_id][STATUS_KEY] = ACK_RECV
            self._map[req_id][NUM_ACK_KEY] += 1
        elif msg == "ready":
            self._map[req_id][STATUS_KEY] = READY

    def __handleQueue(self):
        print("handle queue itself")
        while True:
            time.sleep(0.001)
            # print(f"queue: {self.id}: {self.queue}: {self._map}")
            # print("order ", self.id, "  ", self.__order)

            if not self.queue:

                with open("order_result_" + str(self.id) + ".txt", "w") as f:
                    for item in self.__order:
                        f.write(item + "\n")
                    f.write("file written to : " + str(self.id))
                print("COMPLETE")
                continue

            QUEUE_LOCK.acquire()

            process_req_id = self.queue[0][1]

            if self._map[process_req_id][REQ_FROM_KEY] == 'server':

                # IF the status is 'recv'
                if self._map[process_req_id][STATUS_KEY] == RECV:

                    # Find that controlet
                    controlet_id = self.queue[0][0][1]

                    # Could be a better way?
                    controled_address = self.__controlets[controlet_id]

                    # Send acknowledgement
                    message = self.__createMessage(
                        ['req', process_req_id, self.__getClock(), 'ack'])

                    self.__sendMessage(controled_address, message)

                    # Change the status to ACK_SENT
                    self._map[process_req_id][STATUS_KEY] = ACK_SENT

            # If the staus is 'ACK_SENT' do noting
            if self._map[process_req_id][STATUS_KEY] == ACK_SENT:
                pass

            if self._map[process_req_id][STATUS_KEY] == ACK_RECV:

                if self._map[process_req_id][NUM_ACK_KEY] == len(self.__controlets) - 1:
                    self._map[process_req_id][STATUS_KEY] = READY

                    message = self.__createMessage(
                        ['req', process_req_id, self.__getClock(), 'ready'])

                    self.__broadcastMessage(message)
            if self._map[process_req_id][STATUS_KEY] == READY:
                process_req = self.queue.pop(0)

                request = self._map.pop(process_req_id, None)

                print(f"processing on {self.id}: {process_req} : {request}")

                conn = None
                if request[REQ_FROM_KEY] == 'client':
                    conn = request['conn']

                key = request[KEY_KEY]
                response = ""

                if request[REQ_TYPE] == 'set':
                    value = request[VALUE_KEY]

                    print("set request", key, value)
                    response = self.__setKey(key, value, conn)
                if request[REQ_TYPE] == 'get':
                    print("get request", key)
                    response = self.__getKey(key, conn)

                self.__order.append(
                    f"{process_req_id} {request[REQ_TYPE]} {response}")
            QUEUE_LOCK.release()

    def run(self):
        print("Controlet process started")

        # Start the handling queue thread
        threading.Thread(target=self.__handleQueue, args=()).start()

        # Start the listening process.
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        serverSocket.bind(self.address)

        serverSocket.listen()

        while True:
            try:
                conn, addr = serverSocket.accept()

                request = conn.recv(PAYLOAD_SIZE)

                request = request.decode('utf8')

                if request[:3] == "set":
                    split_resp = request.split(" ")

                    threading.Thread(target=self.__handleClientSet, args=(
                        conn, split_resp[1], split_resp[2].split('\r\n')[0],)).start()
                    # conn.close()

                elif request[:3] == "get":
                    split_resp = request.split(" ")
                    threading.Thread(target=self.__handleClientGet, args=(
                        conn, split_resp[1].split('\r\n')[0],)).start()
                    # conn.close()

                elif request[:3] == "req":
                    threading.Thread(target=self.__handleReq,
                                     args=(request,)).start()
                    conn.close()

                else:
                    conn.sendall(b"INVALID_COMMAND\r\n")
            except Exception as e:
                print(f"Exception raised in: {str(self.__getClock())}")
                print(e)
