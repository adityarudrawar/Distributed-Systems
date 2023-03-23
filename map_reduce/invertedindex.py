from mapreduce import MapReduce
def map(key: str, value: str) -> tuple[str, list[str]]:
    """
    key: document name
    value: word
    """
    return value, key

def reduce(key, value) -> str:
    """
    key: word
    values: occurance
    """
    values = set()
    for val in value:
        values.add(val)

    return key, list(values)

if __name__=="__main__":

    mapReduce = MapReduce(
        mapperFunction= map,
        reducerFunction = reduce,
        numberOfReducers = 3,
        outputFileName = "invertedindex1",
        splitSize = 65536,
        outputFileDirectory = "C:\Work\Projects\Distributed_Systems\map_reduce\output",
        inputFilesLocation= ["C:\Work\Projects\Distributed_Systems\map_reduce\Input_files\sample_count.txt", "C:\Work\Projects\Distributed_Systems\map_reduce\Input_files\sample_count_1.txt"]
        )
    
    mapReduce.start()
    
    