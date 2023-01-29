import socket
import os
from dotenv import load_dotenv
import time
import random
import string
import threading
import pdb
from pymemcache.client.base import Client

# client = Client('localhost')
# client.set('some_key', 'some_value')
# result = client.get('some_key')

if __name__ == "__main__":
    try:
        print("client intialized")

        load_dotenv()

        CLIENT_HOST = os.getenv("CLIENT_HOST")
        CLIENT_PORT = int(os.getenv("CLIENT_PORT"))
        PAYLOAD_SIZE = int(os.getenv("PAYLOAD_SIZE"))

        

        keys = ['CQLYFMJLNE','JNDYNVQWMV','KJZBRUSZCR','TACLEQSQNW','QIIUVMOWLW','YAZCQYCLXJ','IBNOODKYMR','FSHPTJBIFB','ZBQOTCMYHD','EGZYKOYULL','MBMPVMHSTC','DRGEHQSTQP','NNFCMXRKHF','PFOWBKXQNI','CPBNOKCFQS','BHARUKNYVM','XFILOPALJO','KFGECLUFYL','JSXFTCNUVZ','AYXFWKPIIO','EIFAOPDKNU','QDQBRITGAF','XTFDXGMVZR','HYCIJULFUD','AJMILUGQVF','IRHLHTCCAR','CAGOOWJGWM','JWLPSWDEWL','VHECOVCHQZ','KCMMKBUUKE','DEHOLYPAZM','DJYAFQPQPB','VOFBAZJNYI','WYOTBGHKEU','BNCXIUPFWP','GHMGRDCZMG','HZUMUQBWOV','BPNROAWYGL','QYVIVMLXBL','TTUFCMCTKA','PHFRMWLOUF','SQPWCOUNEN','EWFEAMDXRC','JLXWPARRWE','RPZQCLSGTG','DWLLHQTXID','YRNOHFIHFH','OGUQMOLXHQ','VWBEFHQPVL','NBRCXFGTPV']

        threads = []
        generatedKeys = []

        # ------------------SET-----------------------
        def setKeyValue(i):
            
            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            clientSocket.connect((CLIENT_HOST, CLIENT_PORT))    
            
            # setKey = ''.join(random.choices(string.ascii_uppercase, k=10))
            # generatedKeys.append(setKey)
            setKey = keys[i]
            setValue = ''.join(random.choices(string.ascii_lowercase, k=50))


            setCommand = f"set {setKey} {len(setValue)} \r\n"
            
            clientSocket.sendall(setCommand.encode())
            time.sleep(1)
            clientSocket.sendall(setValue.encode() + b' \r\n')

            data = clientSocket.recv(PAYLOAD_SIZE)

            print(f"data received for {setKey}: {data}")
            
            clientSocket.close()


        for i in range(50):
            t = threading.Thread(target=setKeyValue, args=(i,))
            threads.append(t)
            # t.start()

            # setKeyValue(i)

        

        # with open("keys.txt", "w") as txt_file:
        #     for line in generatedKeys:
        #         txt_file.write("".join(line) + "\n")

        # exit()

        #--------------------------------GET------------------------
        def getKeyValue(key):
            
            clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            clientSocket.connect((CLIENT_HOST, CLIENT_PORT))    

            getCommand = f"get {key}\r\n"

            clientSocket.sendall(getCommand.encode())

            response = clientSocket.recv(PAYLOAD_SIZE)


            if response != b"END\r\n":
                response = response.decode()

                response = response.split(" ")
                
                value = b''
                receivedData = clientSocket.recv(PAYLOAD_SIZE)

                while receivedData != b"END\r\n":
                    value += receivedData
                    receivedData = clientSocket.recv(PAYLOAD_SIZE)
                
                print(f"key: {key} Received value from client: {value}")
            else:
                print(f"No data for key: {key} response received: {response}")
            clientSocket.close()

        for i in range(50):
            key = random.choice(keys)
            t = threading.Thread(target=getKeyValue, args=(key,))
            threads.append(t)
            # t.start()

            # getKeyValue(key)
        
        random.shuffle(threads)

        for t in threads:
            t.start()
        
        for t in threads:
            t.join()

        getKeyValue("Aditya")
        print("client closed")
    except Exception as e:
        print(e)
    
    pdb.set_trace()