import socket
from sequencer_server import SequencerServer 
from primary_server import PrimaryServer

class DistributedSequencer:
    def __init__(self, numOfServers = 3):
        print("Distributed Sequencer")
        self.numOfProcessor = numOfServers
        self.__servers = []

        self.__initializeServers()

        self.__startServers()

    def getServers(self):
            return self.__servers

    def __getFreePort(self):
            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp.bind(('', 0))
            _, port = tcp.getsockname()
            tcp.close()
            return port 

    def __initializeServers(self):

        print("Intializing servers: Setting up primary server")
        self.__primaryServer = ( "127.0.0.1" , self.__getFreePort())
        
        print("Intializing servers: Setting up secondary server")
        # Three ports are initalized, need to change the logic later about auto selection of ports
        for i in range(self.numOfProcessor):
                print(".... ", i + 1)
                self.__servers.append(( "127.0.0.1" ,self.__getFreePort(), i + 1))

        print("Intializing servers: Setting up completed")

    def __startServers(self):

        print("Starting the primary server")
        # Start the primary  server
        PrimaryServer(self.__primaryServer[0], self.__primaryServer[1], 0, self.__servers).start()


        print("Starting the secondary servers")
        # Start the sequencer server
        for host, port, id in self.__servers:
            ss = SequencerServer(host=host, port=port, id=id, primaryServer= self.__primaryServer)
            ss.start() 
        
        print("Starting the servers: Completed")