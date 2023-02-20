from mapper import Mapper
from reducer import Reducer
from multiprocessing import Process

class Master(Process):
    def __init__(self):
        print("Master initialized")