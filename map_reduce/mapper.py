from multiprocessing import Process
from task import Task

class Mapper(Process, Task):
    def __init__(self) -> None:
        super().__init__()
        print("Mapper initalized")

    