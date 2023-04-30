from multiprocessing import Process, Queue
from pymemcache.client.base import Client
import socket
import threading
import os
import base64
import time
import mmh3

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

EVENTUAL_CONSISTENCY = 'eventual_consistency'
LINEARIZABLE_CONSISTENCY = 'linearizable_consistency'
SEQUENTIAL_CONSISTENCY = 'sequential_consistency'
CAUSAL_CONSISTENCY = 'causal_consistency'

class Controlet(Process):
    def __init__(self, address, id, datalets, controlets, consistency, output_directory):
        super(Controlet, self).__init__()

        self.__consistency = consistency

        self.address = address

        self.counter = 1

        self.id = id

        self.node_datalet = datalets[id]

        self.queue = []

        self.other_datalets = datalets[:id] + datalets[id + 1:]
        self.__controlets = controlets

        self.__numOfControlets = len(controlets)

        self._map = {}

        self.__order = []

        self.__output_directory = output_directory

        self.__delimeter = " "
    
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

    def __handleSetBroadcast(self, conn, key, value, flags=0, expiry=0, noReply=True):
        self.__incrementCounter()
        logical_clock = str(self.__getClock())

        req_id = self.__getUniqueId()

        self._map[req_id] = {STATUS_KEY: SENT,
                             KEY_KEY: key, VALUE_KEY: value, NUM_ACK_KEY: 0, 'conn': conn, REQ_TYPE: 'set', REQ_FROM_KEY: 'client'}

        self.__appendToQueue(
            [[int(logical_clock.split(".")[0]), int(logical_clock.split(".")[1])], req_id])

        message = self.__createMessage(
            ['req', req_id, logical_clock, 'set', key, value])

        self.__broadcastMessage(message)

    def __handleGetBroadcast(self, conn, key):
        req_id = self.__getUniqueId()

        self.__incrementCounter()

        self._map[req_id] = {STATUS_KEY: SENT,
                             KEY_KEY: key, NUM_ACK_KEY: 0, 'conn': conn, REQ_TYPE: 'get', REQ_FROM_KEY: 'client'}

        logical_clock = str(self.__getClock())

        self.__appendToQueue(
            [[int(logical_clock.split(".")[0]), int(logical_clock.split(".")[1])], req_id])

        # TODO: better the key value size split that is not with a " ". This will cause a lot of errors in the future
        message = self.__createMessage(
            ['req', req_id, logical_clock, 'get', key])

        self.__broadcastMessage(message)

    def __handleSetNormal(self, conn, key, value, flags=0, expiry=0, noReply=True):
        hash_value = mmh3.hash(key)
        hashcode = (hash_value % self.__numOfControlets)
                
        if hashcode == self.id:
            
            self.__incrementCounter()

            counter = self.counter

            response = self.__setKey(key=key, value=value, conn=conn, flags=counter, expiry=expiry, noReply=noReply, address = self.node_datalet)

            for datalet_address in self.other_datalets:
                self.__setKey(key, value, flags= counter, expiry=expiry, noReply=noReply, address = datalet_address)
        else:        
            assigned_controlet = self.__controlets[hashcode]
            self.__setKey(key, value, conn=conn, address = assigned_controlet)


    def __handleGetNormal(self, conn, key):
        self.__getKey(key, conn, address= self.node_datalet)
    
    def __setKey(self, key, value, conn=None, flags=0, expiry=0, noReply=False, address = None):
        '''
        Sends the key value to the datalet
        '''
        c = Client(address)
        response = c.set(key, value, flags=flags, expire=expiry, noreply=noReply)
        if conn != None:
            if response == None or not response:
                conn_response = b"NOT_STORED\r\n"
            else:
                conn_response = b"STORED\r\n"
            conn.sendall(conn_response)
            conn.close()
        c.close()

        return response

    def __getKey(self, key, conn=None, address = None):
        '''
        Gets the value from the datalet
        '''
        c = Client(address)
        response = c.get(key)

        if conn != None:
            # Do I need to do the same thing for SET?
            if response != None:
                conn_response  = response.decode('utf8')

                cmd = 'VALUE' + ' ' + key + ' ' + str(0) + ' ' + str(len(conn_response.encode('utf8'))) + '\r\n'
                conn.send(cmd.encode('utf8'))
                conn.send("{}\r\n".format(conn_response).encode('utf8'))

            conn.send("END\r\n".encode('utf8'))

        c.close()

        return response

    def __createMessage(self, params):
        if len(params) < 4:
            raise Exception

        message = b''
        for i in range(len(params)):
            par = params[i]
            message += str(par).encode('utf8')

            if i == len(params) - 1:
                continue
            message += self.__delimeter.encode('utf8')

        return message

    def __handleReq(self, request):
        # Req, req_id, timestamp, msg, ...
        split_req = request.split(self.__delimeter)
        req, req_id, logical_clock, msg = split_req[0], split_req[1], \
            split_req[2], split_req[3]

        if msg == "set":
            key = split_req[4]
            value = split_req[5]

            self._map[req_id] = {STATUS_KEY: RECV,
                                 KEY_KEY: key, VALUE_KEY: value, REQ_TYPE: msg, REQ_FROM_KEY: 'server'}

            self.__appendToQueue(
                [[int(logical_clock.split(".")[0]), int(logical_clock.split(".")[1])], req_id])

        elif msg == "get":
            key = split_req[4]

            self._map[req_id] = {STATUS_KEY: RECV,
                                 KEY_KEY: key, REQ_TYPE: msg, REQ_FROM_KEY: 'server'}

            self.__appendToQueue(
                [[int(logical_clock.split(".")[0]), int(logical_clock.split(".")[1])], req_id])

        # For the original sender
        elif msg == "ack":
            self._map[req_id][STATUS_KEY] = ACK_RECV
            self._map[req_id][NUM_ACK_KEY] += 1
        elif msg == "ready":
            self._map[req_id][STATUS_KEY] = READY

    def __handleQueue(self):
        while True:
            time.sleep(0.001)

            if not self.queue:

                with open(self.__output_directory + "\order_result_" + str(self.id) + ".txt", "w") as f:
                    for item in self.__order:
                        f.write(item + "\n")
                    f.write("file written to : " + str(self.id))
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

                conn = None
                if request[REQ_FROM_KEY] == 'client':
                    conn = request['conn']

                key = request[KEY_KEY]
                response = ""

                if request[REQ_TYPE] == 'set':
                    value = request[VALUE_KEY]

                    response = self.__setKey(key, value, conn, address=self.node_datalet)

                if request[REQ_TYPE] == 'get':
                    response = self.__getKey(key, conn, address= self.node_datalet)

                self.__order.append(
                    f"{process_req_id} {request[REQ_TYPE]} {response}")
            QUEUE_LOCK.release()

    def run(self):
        if self.__consistency == LINEARIZABLE_CONSISTENCY:
            self.__handleClientSet = self.__handleSetBroadcast
            self.__handleClientGet = self.__handleGetBroadcast

            # Start the handling queue thread
            threading.Thread(target=self.__handleQueue, args=()).start()

        elif self.__consistency == SEQUENTIAL_CONSISTENCY:
            self.__handleClientSet = self.__handleSetBroadcast
            self.__handleClientGet = self.__handleGetNormal

            # Start the handling queue thread
            threading.Thread(target=self.__handleQueue, args=()).start()

        else:
            self.__handleClientSet = self.__handleSetNormal
            self.__handleClientGet = self.__handleGetNormal

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
                    args = getArgsFromRequest(request)
                    threading.Thread(target=self.__handleClientSet, args=(
                        conn, args[0], args[1], args[2], args[3], args[4])).start()
                    # conn.close()

                elif request[:3] == "get":
                    args = getArgsFromRequest(request)

                    threading.Thread(target=self.__handleClientGet, args=(
                        conn, args[0],)).start()
                    # conn.close()

                elif request[:3] == "req":
                    threading.Thread(target=self.__handleReq,
                                     args=(request,)).start()
                    # conn.close()

                else:
                    conn.sendall(b"INVALID_COMMAND\r\n")
            except Exception as e:
                pass

def getArgsFromRequest(request):
    if request[:3] == 'get':
        key = request.split(' ')[1].replace('\r\n', '')
        return [key]

    tokenPos = request.find('\r\n')

    commandList = request.split('\r\n')

    before = commandList[0]
    after = commandList[1]

    before = before.split(" ")

    key = before[1]
    flags = int(before[2])
    expiry = int(before[3])

    if len(before) <= 5:
        noReply = False
        valueSize = int(before[4].split("\r\n")[0])
        value = after
    else:
        noReply = True
        valueSize = int(before[4])
        value = after

    # key: Key,
    # value: Any,
    # expire: int = 0,
    # noreply: bool | None = None,
    # flags: int | None = None
    return [key, value, flags, expiry, noReply]
