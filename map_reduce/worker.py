from utilities import States, getUID
from mapper import Mapper
from multiprocessing import Process

# RPC call between worker and master.
class Worker(Process):
    def __init__(self) -> None:
        self.__uid = getUID()
        self.__state = States.IDLE
        print(f"Worker Initialized: {self.__uid}")

        # Sets up a connection with the Key value store.
    
    def getId(self):
        return self.__uid
    
    def getState(self):
        return self.__state
    
    def __setState(self, state: States):
        self.__state = state

    def runMapper(self, mapperTaskId):
        self.__setState(States.INPROGRESS)
        mapper = Mapper()


        self.__state(States.COMPLETED)
    
    def runReducers(self, mapperOutPuts):
        pass
    
    # This will start the process
    def run():
        print("Run function")