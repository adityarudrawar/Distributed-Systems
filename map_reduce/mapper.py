from multiprocessing import Process
from utilities import readFromOffsets
from keyvalue import KeyValueClient
import concurrent.futures
from utilities import States

class Mapper(Process):
    def __init__(self, 
                task, 
                mapperFunction, 
                outputKey, 
                statusKey,
                firstDelimeter,
                secondDelimeter) -> None:
        
        super(Mapper, self).__init__()

        self.mapperTask = task

        self.__mapperFunction = mapperFunction

        self.__kvc = KeyValueClient()

        self.__outputKey = outputKey

        self.__mapperOutput = []

        self.__mapperStatusKey = statusKey
 
        self.__kvc.set(key = self.__mapperStatusKey, value = States.IDLE.value)

        self.firstDelimeter = firstDelimeter
        self.secondDelimeter = secondDelimeter
    
    def __getChunkPointers(self):
        chunkPointers = self.__kvc.get(key = self.mapperTask)
        return chunkPointers


    def __loadChunk(self, chunkPointer):
        if chunkPointer == "":
            return

        split = chunkPointer.split(self.firstDelimeter)

        location = split[1]
        start = int(split[2])
        end = int(split[3])
        chunk = readFromOffsets(location, start, end)

        if chunk == None:
            return ""
        
        return chunk.decode("utf8"), location

    def __cleanChunk(self, chunk):
        cleanChunk = []

        chunk = chunk.replace("\r", " ")
        chunk = chunk.replace("\n", " ")
        
        chunk = chunk.split(" ")

        for word in chunk:
            word = word.replace(",", "")
            word = word.replace(".", "")
            word = word.replace("!", "")
            word = word.replace("'", "")
            word = word.replace('"', "")
            word = word.replace('(', "")
            word = word.replace(')', "")
            word = word.replace('[', "")
            word = word.replace(']', "")
            word = word.replace('{', "")
            word = word.replace('}', "")
            word = word.replace('-', "")
            word = word.replace(':', "")
            word = word.replace('?', "")
            
            if word == "":
                continue
            
            cleanChunk.append(word)
        
        return cleanChunk

    def __performMapper(self, chunk, location):

        split = self.__cleanChunk(chunk)

        for word in split:
            key, value = self.__mapperFunction(location, word)
            self.__mapperOutput.append(f"{key}{self.secondDelimeter}{value}")
    
    def __storeToKV(self):
        value = ""
        for output in self.__mapperOutput:
            value +=  self.firstDelimeter + output
        
        self.__kvc.set(key = self.__outputKey, value = value)


    def run(self):
        try:
            self.__kvc.set(key = self.__mapperStatusKey, value = States.INPROGRESS.value)
            
            chunkPointers = self.__getChunkPointers()

            chunkData, location = self.__loadChunk(chunkPointers)

            self.__performMapper(chunkData, location)

            self.__storeToKV()

            self.__kvc.set(key = self.__mapperStatusKey, value = States.COMPLETED.value)
        except Exception as e:
            self.__kvc.set(key = self.__mapperStatusKey, value= States.FAILED.value)