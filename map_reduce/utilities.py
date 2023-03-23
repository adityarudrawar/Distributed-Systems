from enum import Enum
import random
import string

MAPPER_INPUT_KEY = "mapper_task_input_"
MAPPER_OUTPUT_KEY = "mapper_task_output_"
MAPPER_STATUS_KEY = "mapper_status_"
REDUCER_OUTPUT_KEY = "reducer_task_output_"
REDUCER_STATUS_KEY = "reducer_status_"

class States(Enum):
    IDLE = '0'
    INPROGRESS = '1'
    COMPLETED = '2'
    FAILED = '3'


def getUID():
    return ''.join(random.choices(string.digits + string.ascii_letters, k=6))


def processInput(fileLocations, splitSize):
    try:
        if type(fileLocations) == str:
            fileLocations = [fileLocations]

        # Storing the offsets
        offSets = []
        
        for location in fileLocations:
            start = 0
            
            # opening the file
            fp = open(location, "rb")
            
            # Getting the end position
            endPosition = fp.seek(0, 2)

            # Setting the pointer to the start
            fp.seek(0, 0)

            while(fp.tell() < endPosition):

                chunk = fp.read(splitSize)

                if chunk == b"":
                    break

                # chunk = chunk.decode()

                k = 0
                delimeters = [b' ', b'\n', b'\r', b'.']
                while(chunk != b"" and chunk not in delimeters and  fp.tell() < endPosition):
                    # fp.seek(start, 0)
                    # k += 1
                    chunk = fp.read(1)
                    # chunk = chunk.decode()

                end = fp.tell()

                offSets.append([location, start, end])
                start = end

            # Close the file
            fp.close()

        return offSets
    except Exception as e:
        print("Exception raised processInput")
        print(e)
        return []
    
def readFromOffsets(location, start, end):

    fp = open(location, "rb")

    fp.seek(start, 0)

    chunk = fp.read(end - start)

    fp.close()
    return chunk