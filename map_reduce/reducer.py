from multiprocessing import Process
from utilities import States
import concurrent.futures
from keyvalue import KeyValueClient
import hashlib

class Reducer(Process):
    def __init__(self,
        reducerFunction,
        outputKey,
        statusKey,
        reducerId,
        mapperOutputs,
        numberOfReducers,
        firstDelimeter,
        secondDelimeter) -> None:

        super(Reducer, self).__init__()
        
        print("Reducer initalized")

        self.__reducerId = reducerId

        self.__reducerFunction = reducerFunction

        self.__outputKey = outputKey

        self.__reducerStatusKey = statusKey

        self.__kvc = KeyValueClient()

        self.__mapperOutputsKey = mapperOutputs

        self.__kvc.set(key = self.__reducerStatusKey, value = States.IDLE.value)

        self.__reducerOutput = ""

        self.__intermediateData = []

        self.__groupByData = {}

        self.__numberOfReducers = numberOfReducers

        self.firstDelimeter = firstDelimeter
        self.secondDelimeter = secondDelimeter

    def __getMapperOutputs(self):
        for i in self.__mapperOutputsKey:
            response = self.__kvc.get(key = i)
            self.__intermediateData.append(response)

    def __getGroupByData(self):
        hashMap = {}
        for data in self.__intermediateData:
            data = data.split(self.firstDelimeter)
            for keyValue in data:
                
                if keyValue == "":
                    continue

                key = keyValue.split(self.secondDelimeter)[0]
                value = keyValue.split(self.secondDelimeter)[1]
                
                hashKey = key.encode("utf8")
                h = hashlib.sha256(hashKey)

                if int(h.hexdigest(), base=16) % self.__numberOfReducers != self.__reducerId:
                    continue
                
                if key not in hashMap:
                    hashMap[key] = []
                hashMap[key].append(value)

        self.__groupByData = hashMap

    def __performReducer(self):
        for key in self.__groupByData.keys():
            result = self.__reducerFunction(key, self.__groupByData[key])
            self.__reducerOutput += f"{self.firstDelimeter}" + f"{key}{self.secondDelimeter}{result}"

    def __storeToKV(self):
        self.__kvc.set(key = self.__outputKey, value = self.__reducerOutput)


    def run(self):
        try:
            self.__kvc.set(key =self.__reducerStatusKey, value = States.INPROGRESS.value)

            self.__getMapperOutputs()

            self.__getGroupByData()

            self.__performReducer()

            self.__storeToKV()

            self.__kvc.set(key = self.__reducerStatusKey, value = States.COMPLETED.value)
        except Exception as e:
            self.__kvc.set(key = self.__reducerStatusKey, value= States.FAILED.value)