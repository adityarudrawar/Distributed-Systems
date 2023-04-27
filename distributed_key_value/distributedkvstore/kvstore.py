import socket
from distributedkvstore import datalet
from distributedkvstore import controlet


EVENTUAL_CONSISTENCY = 'eventual_consistency'
LINEARIZABLE_CONSISTENCY = 'linearizable_consistency'
SEQUENTIAL_CONSISTENCY = 'sequential_consistency'
CASUAL_CONSISTENCY = 'casual_consistency'

class KVStore:
    def __init__(self, replicas = 3,
                 consistency= '',
                 storage_directory = ""):
        print("Intializing KV STORE")

        if consistency == '':
            print("consistency model not found")
            return

        self.replicas = replicas

        # A list of tuple of controlet and datalet ports
        self.__node_ports = []

        self.host = '127.0.0.1'
        
        self.storage_directory = storage_directory

    def get_controlet_address(self):
        return self.__controlet_addresses

    def start(self):
        print("Collecting ports for controlets and datalets")
        controlets_ports = []
        datalets_ports = []

        for i in range(self.replicas):
            c_port = get_free_port()
            d_port = get_free_port()

            controlets_ports.append(c_port)  
            datalets_ports.append(d_port)   

        self.__datalet_addresses = []
        datalets = []       
        print("Initializing datalets")
        for i in range(len(datalets_ports)):
            d = datalet.Datalet(address=(self.host, datalets_ports[i]), storage_directory= self.storage_directory, id = i)
            datalets.append(d)  
            self.__datalet_addresses.append((self.host, datalets_ports[i]))

        print("Starting datalets")
        for d in datalets:
            d.start()      
        
        self.__controlet_addresses = []
        controlets = []
        print("Initializing controlets")
        for i in range(len(controlets_ports)):
            c = controlet.Controlet(address=(self.host, controlets_ports[i]), id = i, datalets= self.__datalet_addresses)   
            controlets.append(c)
            self.__controlet_addresses.append((self.host, controlets_ports[i])) 

        print("Starting controlets")
        for c in controlets:
            c.start()

        print("Controlets and Datalets initialized")


def get_free_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    _, port = tcp.getsockname()
    tcp.close()
    return port 