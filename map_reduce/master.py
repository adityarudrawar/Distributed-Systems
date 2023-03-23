from multiprocessing import Process
from mapper import Mapper
from reducer import Reducer
from utilities import States
from keyvalue import KeyValueClient
import random

class Master(Process):
    def __init__(self, 
                mapperTasksList, 
                mapperFunction, 
                reducerFunction, 
                numberOfMappers, 
                numberOfReducers, 
                mapperInputKey, 
                mapperOutputKey, 
                reducerOutputKey, 
                mapperStatusKey, 
                reducerStatusKey,
                firstDelimeter,
                secondDelimeter 
                ):
        """
        1. Assign each worker a mapper job.
        2. Co-ordinate between each mapper.
        3. Extra: Add Fault toularance for each worker.
        4. kill the worker processes.
        5. Start the reducers.
        """
        super(Master, self).__init__()
        print("Master initialized")

        self.mapperTasksList = mapperTasksList

        self.mapperOutputs = []
        
        self.__mapperFunction = mapperFunction
        self.__reducerFunction = reducerFunction

        self.numberOfMappers = numberOfMappers
        self.numberOfReducers = numberOfReducers

        self.__kvc = KeyValueClient() 
        
        self.mapperInputKey = mapperInputKey
        self.mapperOutputKey = mapperOutputKey
        self.reducerOutputKey = reducerOutputKey
        self.mapperStatusKey = mapperStatusKey
        self.reducerStatusKey = reducerStatusKey

        self.firstDelimeter = firstDelimeter
        self.secondDelimeter = secondDelimeter

    def __runMappers(self):
        inCompleteTasks = set()
        for id in self.mapperTasksList:
            inCompleteTasks.add(id)


        currentlyRunning = set()

        while(len(inCompleteTasks) != 0):
            
            while(len(currentlyRunning) < self.numberOfMappers):
                # Gets any task id from the inCompleteTasks set
                try:
                    taskId = random.choice(list(inCompleteTasks - currentlyRunning))
                except Exception as e:
                    taskId = None

                if taskId == None:
                    break
                
                mapper = Mapper(task = f"{self.mapperInputKey}{taskId}", 
                                mapperFunction = self.__mapperFunction, 
                                
                                outputKey = f"{self.mapperOutputKey}{taskId}",
                                statusKey = f"{self.mapperStatusKey}{taskId}",
                                firstDelimeter = self.firstDelimeter,
                                secondDelimeter = self.secondDelimeter
                )

                mapper.start()

                # Mapper is started
                currentlyRunning.add(taskId)

            # Decrement currentlyRunning when a mapper fails or completes
            # If a mapper completes then remove the id from inCompleteTasks
            failedTasks = set()
            completedTasks = set()
            for taskId in currentlyRunning:
                status = self.__kvc.get(key = f"{self.mapperStatusKey}{taskId}")
                if status == None or status == States.FAILED.value:
                    failedTasks.add(taskId)
                elif status == States.COMPLETED.value:
                    completedTasks.add(taskId)
                    self.mapperOutputs.append(f"{self.mapperOutputKey}{taskId}")
                    

            currentlyRunning = currentlyRunning - failedTasks
            currentlyRunning = currentlyRunning - completedTasks

            inCompleteTasks = inCompleteTasks - completedTasks

        print("Mappers completed, barrier down")

    def __runReducers(self):
        inCompleteTasks = set()
        for id in range(self.numberOfReducers):
            inCompleteTasks.add(id)


        currentlyRunning = set()

        while(len(inCompleteTasks) != 0):
            
            while(len(currentlyRunning) < self.numberOfReducers):
                # Gets any task id from the inCompleteTasks set
                try:
                    id = random.choice(list(inCompleteTasks - currentlyRunning))
                except Exception as e:
                    id = None

                if id == None:
                    break

                reducer = Reducer(
                                reducerFunction= self.__reducerFunction, 
                                outputKey = f"{self.reducerOutputKey}{id}",
                                statusKey = f"{self.reducerStatusKey}{id}",
                                reducerId= id,
                                mapperOutputs= self.mapperOutputs,
                                numberOfReducers= self.numberOfReducers,
                                firstDelimeter = self.firstDelimeter,
                                secondDelimeter = self.secondDelimeter
                )

                reducer.start()

                # Mapper is started
                currentlyRunning.add(id)

            # Decrement currentlyRunning when a reducer fails or completes
            # If a reducer completes then remove the id from inCompleteTasks
            failedTasks = set()
            completedTasks = set()
            for id in currentlyRunning:
                status = self.__kvc.get(key = f"{self.reducerStatusKey}{id}")
                if status == None:
                    failedTasks.add(id)
                elif status == States.COMPLETED.value:
                    completedTasks.add(id)

            currentlyRunning = currentlyRunning - failedTasks
            currentlyRunning = currentlyRunning - completedTasks

            inCompleteTasks = inCompleteTasks - completedTasks

    def run(self):
        
        self.__runMappers()

        self.__runReducers()