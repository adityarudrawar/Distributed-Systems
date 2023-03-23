from mapreduce import MapReduce
def map(key: str, value: str) -> tuple[str, list[str]]:
    """
    key: document name
    value: word
    """
    return value, "1"

def reduce(key, value) -> str:
    """
    key: word
    values: occurance
    """
    values = []
    for val in value:
        values.append(int(val))

    return str(sum(values))

if __name__=="__main__":

    mapReduce = MapReduce(
        mapperFunction= map,
        reducerFunction = reduce,
        numberOfReducers = 5,
        splitSize = 4096,
        outputFileName = "wordcount1",
        outputFileDirectory= "C:\Work\Projects\Distributed_Systems\map_reduce\output",
        inputFilesLocation= ["C:\Work\Projects\Distributed_Systems\map_reduce\Input_files\sample_count.txt"]
    )
    
    mapReduce.start()