from master import Master
from utilities import processInput, getUID, MAPPER_INPUT_KEY, MAPPER_OUTPUT_KEY, REDUCER_OUTPUT_KEY, MAPPER_STATUS_KEY, REDUCER_STATUS_KEY
from keyvalue import KeyValueClient
import os
import time
from datetime import datetime

class MapReduce:
    def __init__(self, mapperFunction, reducerFunction, numberOfReducers, splitSize, outputFileDirectory, inputFilesLocation, outputFileName = ""):
        print("Intialized Map reduce")

        # Self id
        self.mrId = getUID()
        print("Map reduce UUID: ", self.mrId)

        # Methods
        self.mapperFunction = mapperFunction
        self.reducerFunction = reducerFunction
        
        # Input and Output file locations
        self.outputFileDirectory = outputFileDirectory
        self.inputFilesLocation = inputFilesLocation

        # No of reducers
        self.numberOfReducers = numberOfReducers
        self.numberOfMappers = 100

        # Size of the split
        self.splitSize = splitSize

        # List of Workers
        self.workers = []

        # Mapper tasks [File Location, [Start, end]]
        # This will be passed to the Master Node, so that the master can assign the mapper task to any process.
        self.fileDistribution = []

        self.mapperTasksList = []

        # Get key value object
        self.__kvc = KeyValueClient()

        self.firstDelimeter = "--"
        self.secondDelimeter = "@@"

        self.outputFileName = outputFileName


    def __processInput(self):
        print("Processing Input: Intialized")
        self.fileDistribution =  processInput(self.inputFilesLocation, self.splitSize)

        print("Storing file pointers to key value")
        for i in range(len(self.fileDistribution)):
            
            mapperTaskId = i
            
            value = ""
            
            for _ in self.fileDistribution[i]:
                value += self.firstDelimeter + str(_)
            
            self.__kvc.set(key= f"{MAPPER_INPUT_KEY}{mapperTaskId}", value= value)
            
            self.mapperTasksList.append(mapperTaskId)
            
        print("Processing Input: Complete")

    def __storeOutput(self):
        
        # Get the output from the key value store
        intermediateOutput = ""
        for i in range(self.numberOfReducers):
            intermediateOutput += self.__kvc.get(key = f"{REDUCER_OUTPUT_KEY}{i}")
        
        # Storing to the output location

        if not os.path.exists(self.outputFileDirectory):
            os.makedirs(self.outputFileDirectory)
        
        dataSplit = intermediateOutput.split(self.firstDelimeter)

        finalOutput = ""
        for data in dataSplit:
            data = data.split(self.secondDelimeter)
            if len(data) < 2:
                continue
            finalOutput += f"{data[0]}:{data[1]}\n"

        fileName = self.outputFileName + "__" + datetime.now().strftime("%H_%M_%S")+ "_" + self.mrId + "__OUTPUT__"

        fp = open(f"{self.outputFileDirectory}\{fileName}", "w+", encoding="utf8")
        fp.write(finalOutput)
        fp.close()
        

            


    def start(self):

        self.__processInput()
        
        master = Master(mapperTasksList = self.mapperTasksList,
                        mapperFunction = self.mapperFunction,
                        reducerFunction = self.reducerFunction,
                        numberOfMappers = self.numberOfMappers,
                        numberOfReducers = self.numberOfReducers,
                        mapperInputKey = MAPPER_INPUT_KEY,
                        mapperOutputKey = MAPPER_OUTPUT_KEY,
                        reducerOutputKey = REDUCER_OUTPUT_KEY,
                        mapperStatusKey = MAPPER_STATUS_KEY,
                        reducerStatusKey = REDUCER_STATUS_KEY,

                        firstDelimeter = self.firstDelimeter,
                        secondDelimeter = self.secondDelimeter
                        )
        
        # master.run()

        master.start()

        master.join()

        self.__storeOutput()

        print("Map reduce finished")