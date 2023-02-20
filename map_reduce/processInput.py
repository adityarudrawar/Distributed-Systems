import pdb

def processInput(location, splitSize):

    # Storing the offsets
    offSets = []
    
    start = 0
    
    fp = open(location, "r", encoding="utf-8")
    
    # Getting the end position
    endPosition = fp.seek(0, 2)

    # Setting the pointer to the start
    fp.seek(0, 0)


    while(fp.tell() < endPosition):
        
        chunk = fp.read(splitSize)

        if chunk == "":
            break
        
        k = 0
        while(chunk[-1] != " " and fp.tell() < endPosition):
            fp.seek(start, 0)
            k += 1
            chunk = fp.read(splitSize + k)

        end = fp.tell()

        offSets.append([start, end])
        start = end + 1


    fp.close()

    print(offSets)
    return (offSets)


def readFromOffsets(location, offSets):

    outputFile = open("output1.txt", "w+")

    inputFIle = open(location, "r")

    print(offSets)

    for [start, end] in offSets:
        
        chunk = inputFIle.read(end - start + 1)

        if chunk != "" and chunk[-1] != " ":
            print("exception chunk", chunk)

        outputFile.write(chunk)

    inputFIle.close()
    outputFile.close()

location = "C:\Work\Projects\Distributed_Systems\map_reduce\Input_files\input1.txt"
splitSize = 4096

offSets = processInput(location, splitSize)
readFromOffsets(location, offSets)

location = "C:\Work\Projects\Distributed_Systems\map_reduce\Input_files\input2.txt"
splitSize = 4096

offSets = processInput(location, splitSize)
readFromOffsets(location, offSets)

location = "C:\Work\Projects\Distributed_Systems\map_reduce\Input_files\input2.txt"
splitSize = 1

offSets = processInput(location, splitSize)
readFromOffsets(location, offSets)

location = "C:\Work\Projects\Distributed_Systems\map_reduce\Input_files\input2.txt"
splitSize = 5

offSets = processInput(location, splitSize)
readFromOffsets(location, offSets)

location = "C:\Work\Projects\Distributed_Systems\map_reduce\Input_files\input2.txt"
splitSize = 9

offSets = processInput(location, splitSize)
readFromOffsets(location, offSets)

location = "C:\Work\Projects\Distributed_Systems\map_reduce\Input_files\input3.txt"
splitSize = 4096

offSets = processInput(location, splitSize)
readFromOffsets(location, offSets)
