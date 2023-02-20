from master import Master

class MapReduce:
    def __init__(self, mapperFunction, reducerFunction, reducerTasks: int, splitSize: int, outputFileLocation: str, inputFilesLocation):
        print("Intialized Map reduce")

        # Methods
        self.mapperFunction = mapperFunction
        self.reducerFunction = reducerFunction
        
        # Input and Output file locations
        self.outputFileLocation = outputFileLocation
        self.inputFilesLocation = inputFilesLocation

        # No of reducers
        self.reducerTasks = reducerTasks

        # Size of the split
        self.splitSize = splitSize

        # Mapper tasks [File Location, [Start, end]]
        # This will be passed to the Master Node, so that the master can assign the mapper task to any process.
        self.mapperTaskList: list = []

    def start(self):
        """
        1. Split the input files into equivalent pieces
        2. Initialize the master
        3. Start the master
        4. 
        """

    def processInput(self):
        if type(self.inputFilesLocation) == str:
            # Only a single file
            pass
        else:
            # Multiple files
            pass
    
    def __splitFile(location):
        pass