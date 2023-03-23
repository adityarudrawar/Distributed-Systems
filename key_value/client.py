import socket
import os
# from dotenv import load_dotenv
import time
import random
import string
import threading
# import pdb
from pymemcache.client.base import Client
# import matplotlib.pyplot as plt

def calculateAverageTime(array, index):
    totalTime = 0

    for record in array:
        totalTime += record[index]
    avgTime = totalTime/len(array)

    return avgTime

def startAndWaitForThreads(threadArray):
    for t in threadArray:
        t.start()
    
    for t in threadArray:
        t.join()

def plotGraph(x, y, x_axis= "X-axis", y_axis= "Y-axis", title= ""):
    plt.plot(x, y)
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)
    plt.title(title)
    plt.show()

if __name__ == "__main__":
    try:
        print("client intialized")

        # load_dotenv()

        # CLIENT_HOST = os.getenv("CLIENT_HOST")
        # CLIENT_PORT = int(os.getenv("CLIENT_PORT"))
        # PAYLOAD_SIZE = int(os.getenv("PAYLOAD_SIZE"))

        try:
            client = Client("127.0.0.1:65432", default_noreply= True, encoding="utf8")
            # print(client.set("mapper_task_output_3","k1"))
            print(client.get("mapper_task_output_2"))
            client.close()
        except Exception as e:
            print(e)
        exit()

        setAverageTime = []
        getAverageTime = []
        setGetAverageTime = []

        noOfClients = [50, 100, 150, 200, 250, 300, 350, 400]

        for i in noOfClients:
            threads = []
            generatedKeys = []
            
            setResultsPerClient = []
            getResultsPerClient = []
            numberOfClients = i

            client = None

            def setFunction(id, flag):
                
                if flag:
                    setKey = ''.join(random.choices(string.ascii_uppercase, k=10))
                    generatedKeys.append(setKey)
                else:
                    setKey = generatedKeys[id]

                setValue = ''.join(random.choices(string.ascii_lowercase, k=50))
                
                no_reply = random.choice([False, True])
                
                start = time.time()
                
                client = Client(CLIENT_HOST + ":" + str(CLIENT_PORT), default_noreply= no_reply)
                result = client.set(setKey, setValue)

                end = time.time()

                setResultsPerClient.append([setKey,  result, end - start])

                # print(f'SET Key: {setKey} Value: {setValue} ', 'STORED\r\n' if result else 'NOT_STORED\r\n')
                client.close()
                

            def getFunction(key):
                
                start = time.time()

                client = Client(CLIENT_HOST + ":" + str(CLIENT_PORT))
                result = client.get(key)            
                
                end = time.time()

                getResultsPerClient.append([key,  result, end - start])

                # print(f'GET Key: {key} Value: {result}' )
                client.close()

            # Concurrent SET requests
            for i in range(numberOfClients):
                t = threading.Thread(target=setFunction, args=(i, True, ))
                threads.append(t)
            
            # Start and Wait
            startAndWaitForThreads(threads)
        
            # print("-------------------------------------")
            # print('generatedKeys')
            # print(generatedKeys)

            # print("TEST CASE 1: SET REQUESTS")
            # print("-------------------------------------")
            # print('setResultsPerClient')
            # print(setResultsPerClient)

            setAverageTime.append(calculateAverageTime(setResultsPerClient, 2))

            threads = []

            # Concurrent GET requests
            for i in range(numberOfClients):
                t = threading.Thread(target=getFunction, args=(generatedKeys[i], ))
                threads.append(t)

            # Start and Wait
            startAndWaitForThreads(threads)

            # print("TEST CASE 2: GET REQUESTS")
            # print("-------------------------------------")
            # print('getResultsPerClient')
            # print(getResultsPerClient)
            
            getAverageTime.append(calculateAverageTime(getResultsPerClient, 2))

            threads = []
            setResultsPerClient = []
            getResultsPerClient = []

            for i in range(numberOfClients):
                t1 = threading.Thread(target=setFunction, args=(i, False, ))
                threads.append(t1)

                t2 = threading.Thread(target=getFunction, args=(generatedKeys[i], ))
                threads.append(t2)

            random.shuffle(threads)

            # Start and Wait
            startAndWaitForThreads(threads)
            

            # print("TEST CASE 3: SET AND GET REQUESTS")
            # print("-------------------------------------")
            # print('setResultsPerClient')
            # print(setResultsPerClient)

            # print("-------------------------------------")
            # print('getResultsPerClient')
            # print(getResultsPerClient)
            
            setGetAverageTime.append((calculateAverageTime(getResultsPerClient, 2) + calculateAverageTime(setResultsPerClient, 2) )/ 2)

            threads = []
            getResultsPerClient = []

            randomKeys = [''.join(random.choices(string.ascii_uppercase, k=10)) for _ in range(numberOfClients)]

            for i in range(numberOfClients):
                t = threading.Thread(target=getFunction, args=(randomKeys[i],))
                threads.append(t)
            
            # Start and Wait
            startAndWaitForThreads(threads)

            # print("TEST CASE 4: RANDOM KEY REQUESTS")
            # print("-------------------------------------")
            # print('getResultsPerClient')
            # print(getResultsPerClient)

            print("done")

        plotGraph(noOfClients, setAverageTime, "AVG SET request time", "Time", "CONCURRENT SET REQUEST")
        
        plotGraph(noOfClients, getAverageTime, "AVG GET request time", "Time", "CONCURRENT GET REQUEST")
        
        plotGraph(noOfClients, setGetAverageTime, "AVG SET AND GET request time", "Time", "CONCURRENT SET AND GET REQUEST")

    except Exception as e:
        print(e)