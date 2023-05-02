from multiprocessing import Process, Queue
from pymemcache.client.base import Client
import socket
import threading
import os
import base64
import time
import mmh3

STATUS = 'status'
KEY = 'key'
VALUE = 'value'
NUM_ACK = 'ack'
REQ_FROM = 'req_from'

ACK_SENT = 'ack_sent'
ACK_RECV = 'ack_recv'

READY = 'ready'
SENT = 'sent'
RECV = 'recv'

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

        self.__address = address

        self.__counter = 1

        self.__id = id

        self.__node_datalet = datalets[id]

        self.__queue = []

        self.__other_datalets = datalets[:id] + datalets[id + 1:]
        self.__controlets = controlets

        self.__numOfControlets = len(controlets)

        self.__map = {}

        self.__order = []

        self.__output_directory = output_directory

        self.__delimeter = '__&**&__'

    def __getUniqueId(self):
        return base64.b64encode(os.urandom(6)).decode('ascii')

    def __incrementCounter(self):
        COUNTER_LOCK.acquire()
        self.__counter += 1
        COUNTER_LOCK.release()

    def __getClock(self):
        return float(str(self.__counter) + '.' + str(self.__id))

    def __appendToQueue(self, contents):

        QUEUE_LOCK.acquire()

        self.__queue.append(contents)
        self.__queue.sort(key=lambda x: (x[0][0], x[0][1]))

        QUEUE_LOCK.release()

    def __popFromQueue(self):
        QUEUE_LOCK.acquire()

        self.__queue.pop(0)
        self.__queue.sort(key=lambda x: (x[0][0], x[0][1]))

        QUEUE_LOCK.release()

    def __sendMessage(self, address, message):
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect(address)

        clientSocket.sendall(message)

        # Do we need this?
        # clientSocket.recv(PAYLOAD_SIZE)

    def __broadcastMessage(self, message):

        for controlet in self.__controlets:
            if controlet == self.__address:
                continue
            self.__sendMessage(controlet, message)

    def __handleSetBroadcast(self, conn, key, value, flags=0, expiry=0, noReply=True):
        self.__incrementCounter()
        logical_clock = str(self.__getClock())

        req_id = self.__getUniqueId()

        self.__map[req_id] = {STATUS: SENT,
                             KEY: key, VALUE: value, NUM_ACK: 0, 'conn': conn, REQ_TYPE: 'set', REQ_FROM: 'client'}

        self.__appendToQueue(
            [[int(logical_clock.split('.')[0]), int(logical_clock.split('.')[1])], req_id])

        message = self.__createMessage(
            ['req', req_id, logical_clock, 'set', key, value])

        self.__broadcastMessage(message)

    def __handleGetBroadcast(self, conn, key):
        req_id = self.__getUniqueId()

        self.__incrementCounter()

        self.__map[req_id] = {STATUS: SENT,
                             KEY: key, NUM_ACK: 0, 'conn': conn, REQ_TYPE: 'get', REQ_FROM: 'client'}

        logical_clock = str(self.__getClock())

        self.__appendToQueue(
            [[int(logical_clock.split('.')[0]), int(logical_clock.split('.')[1])], req_id])

        message = self.__createMessage(
            ['req', req_id, logical_clock, 'get', key])

        self.__broadcastMessage(message)

    def __handleSetNormal(self, conn, key, value, flags=0, expiry=0, noReply=True):
        hash_value = mmh3.hash(key)
        hashcode = (hash_value % self.__numOfControlets)
                
        if hashcode == self.__id:
            
            self.__incrementCounter()

            counter = self.__counter
            
            # Augmented delay in the network while broadcasting the write request
            # time.sleep(5)

            response = self.__setKey(key=key, value=value, conn=conn, flags=counter, expiry=expiry, noReply=noReply, address = self.__node_datalet)

            for datalet_address in self.__other_datalets:
                self.__setKey(key, value, flags= counter, expiry=expiry, noReply=noReply, address = datalet_address)
        else:        
            assigned_controlet = self.__controlets[hashcode]
            self.__setKey(key, value, conn=conn, address = assigned_controlet)


    def __handleGetNormal(self, conn, key):
        self.__getKey(key, conn, address= self.__node_datalet)
    
    def __setKey(self, key, value, conn=None, flags=0, expiry=0, noReply=False, address = None):
        '''
        Sends the key value to the datalet
        '''
        c = Client(address)
        response = c.set(key, value, flags=flags, expire=expiry, noreply=noReply)
        if conn != None:
            if response == None or not response:
                conn_response = b'NOT_STORED\r\n'
            else:
                conn_response = b'STORED\r\n'
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
            if response != None:
                conn_response  = response.decode('utf8')

                cmd = 'VALUE' + ' ' + key + ' ' + str(0) + ' ' + str(len(conn_response.encode('utf8'))) + '\r\n'
                conn.send(cmd.encode('utf8'))
                conn.send('{}\r\n'.format(conn_response).encode('utf8'))

            conn.send('END\r\n'.encode('utf8'))

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

        split_req = request.split(self.__delimeter)
        req, req_id, logical_clock, msg = split_req[0], split_req[1], \
            split_req[2], split_req[3]

        if msg == 'set':
            key = split_req[4]
            value = split_req[5]

            self.__map[req_id] = {STATUS: RECV,
                                 KEY: key, VALUE: value, REQ_TYPE: msg, REQ_FROM: 'server'}

            self.__appendToQueue(
                [[int(logical_clock.split('.')[0]), int(logical_clock.split('.')[1])], req_id])

        elif msg == 'get':
            key = split_req[4]

            self.__map[req_id] = {STATUS: RECV,
                                 KEY: key, REQ_TYPE: msg, REQ_FROM: 'server'}

            self.__appendToQueue(
                [[int(logical_clock.split('.')[0]), int(logical_clock.split('.')[1])], req_id])

        # For the original sender
        elif msg == 'ack':
            self.__map[req_id][STATUS] = ACK_RECV
            self.__map[req_id][NUM_ACK] += 1
        elif msg == 'ready':
            self.__map[req_id][STATUS] = READY

    def __handleQueue(self):
        while True:
            # time.sleep(0.001)

            if not self.__queue:
                if self.__output_directory != "":
                    with open(self.__output_directory + '\order_result_' + str(self.__id) + '.txt', 'w') as f:
                        for item in self.__order:
                            f.write(item + '\n')
                        f.write('file written to : ' + str(self.__id))
                continue

            QUEUE_LOCK.acquire()

            process_req_id = self.__queue[0][1]

            if self.__map[process_req_id][REQ_FROM] == 'server':

                # IF the status is 'recv'
                if self.__map[process_req_id][STATUS] == RECV:

                    # Find that controlet
                    controlet_id = self.__queue[0][0][1]

                    # Could be a better way?
                    controled_address = self.__controlets[controlet_id]

                    # Send acknowledgement
                    message = self.__createMessage(
                        ['req', process_req_id, self.__getClock(), 'ack'])

                    self.__sendMessage(controled_address, message)

                    # Change the status to ACK_SENT
                    self.__map[process_req_id][STATUS] = ACK_SENT

            # If the staus is 'ACK_SENT' do nothing

            if self.__map[process_req_id][STATUS] == ACK_RECV:

                if self.__map[process_req_id][NUM_ACK] == len(self.__controlets) - 1:
                    self.__map[process_req_id][STATUS] = READY

                    message = self.__createMessage(
                        ['req', process_req_id, self.__getClock(), 'ready'])

                    self.__broadcastMessage(message)
            if self.__map[process_req_id][STATUS] == READY:
                process_req = self.__queue.pop(0)

                request = self.__map.pop(process_req_id, None)

                conn = None
                if request[REQ_FROM] == 'client':
                    conn = request['conn']

                key = request[KEY]
                response = ''

                if request[REQ_TYPE] == 'set':
                    value = request[VALUE]

                    response = self.__setKey(key, value, conn, address=self.__node_datalet)

                if request[REQ_TYPE] == 'get':
                    response = self.__getKey(key, conn, address= self.__node_datalet)

                self.__order.append(f'reqid: {process_req_id} {request[REQ_TYPE]} key {key}  {response}')

            QUEUE_LOCK.release()

    def run(self):
        if self.__consistency == LINEARIZABLE_CONSISTENCY:
            print(f'{self.__id} {LINEARIZABLE_CONSISTENCY}')
            self.__handleClientSet = self.__handleSetBroadcast
            self.__handleClientGet = self.__handleGetBroadcast

            # Start the handling queue thread
            threading.Thread(target=self.__handleQueue, args=()).start()

        elif self.__consistency == SEQUENTIAL_CONSISTENCY:
            print(f'{self.__id} {SEQUENTIAL_CONSISTENCY}')
            self.__handleClientSet = self.__handleSetBroadcast
            self.__handleClientGet = self.__handleGetNormal

            # Start the handling queue thread
            threading.Thread(target=self.__handleQueue, args=()).start()

        else:
            print(f'{self.__id} {EVENTUAL_CONSISTENCY}')
            self.__handleClientSet = self.__handleSetNormal
            self.__handleClientGet = self.__handleGetNormal

        # Start the listening process.
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        serverSocket.bind(self.__address)

        serverSocket.listen()

        while True:
            try:
                conn, addr = serverSocket.accept()

                request = conn.recv(PAYLOAD_SIZE)

                request = request.decode('utf8')

                if request[:3] == 'set':
                    args = getArgsFromRequest(request)
                    threading.Thread(target=self.__handleClientSet, args=(
                        conn, args[0], args[1], args[2], args[3], args[4])).start()
                    # conn.close()

                elif request[:3] == 'get':
                    args = getArgsFromRequest(request)

                    threading.Thread(target=self.__handleClientGet, args=(
                        conn, args[0],)).start()
                    # conn.close()

                elif request[:3] == 'req':
                    threading.Thread(target=self.__handleReq,
                                     args=(request,)).start()
                    # Important to close the connection
                    conn.close()

                else:
                    conn.sendall(b'INVALID_COMMAND\r\n')
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

    before = before.split(' ')

    key = before[1]
    flags = int(before[2])
    expiry = int(before[3])

    if len(before) <= 5:
        noReply = False
        valueSize = int(before[4].split('\r\n')[0])
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
