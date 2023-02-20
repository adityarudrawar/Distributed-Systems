from multiprocessing import Process
from task import Task

class Reducer(Process, Task):
    def __init__(self) -> None:
        super().__init__()
        print("Reducer initalized")