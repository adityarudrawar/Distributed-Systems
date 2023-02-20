from mapreduce import MapReduce

def map(key: str, value: str) -> tuple[str, list[str]]:
    """
    key: document name
    value: word
    """
    
    return value, ["1"]

def reduce(key, values) -> str:
    """
    key: word
    values: occurance
    """
    values = [int(val) for val in values]

    return str(sum(values))

if __name__=="__main__":

    mapReduce = MapReduce(
        mapperFunction= map,
        reducerFunction = reduce,
        reducerTasks = 10,
        splitSize = 4096,
        outputFileLocation = "",
        inputFilesLocation = ""
        )
    
    mapReduce.start()
    
