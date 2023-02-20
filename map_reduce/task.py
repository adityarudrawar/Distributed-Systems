from utilities import States, getUID

class Task:
    def __init__(self) -> None:
        self.__uid = getUID()
        self.__state = States.IDLE
        print(f"Task Initialized: {self.__uid}")
    
    
    def getId(self):
        return self.__uid
    
    def getState(self):
        return self.__state
    
    def setState(self, state: States):
        self.__state = state