from enum import Enum
import random
import string

class States(Enum):
    IDLE = 0
    INPROGRESS = 1
    COMPLETED = 2


def getUID():
    return ''.join(random.choices(string.digits + string.ascii_letters, k=6))