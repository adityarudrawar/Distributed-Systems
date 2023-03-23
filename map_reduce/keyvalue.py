
import configparser
from pymemcache import Client

class KeyValueClient:
    def __init__(self):
        # Creates a client and connects to the server
        try:
            config = configparser.ConfigParser()
            config.read('.env')

            self.SERVER_HOST = config["TEST"]["KEYVALUE_SERVER_HOST"]
            self.SERVER_PORT = int(config["TEST"]["KEYVALUE_SERVER_PORT"])
        except Exception as e:
            print("KeyValue.py KeyValueClient __init__")
            print(e)

    def set(self, key, value):
        client = Client(self.SERVER_HOST + ":" + str(self.SERVER_PORT), default_noreply= True, encoding='utf8')
        if "mapper_task_output_" in key:
            pass
        result = client.set(key, value, expire=0)
        client.close()
        return result


    def get(self, key):
        if 'mapper_task_output_' in key:
            pass
        client = Client(self.SERVER_HOST + ":" + str(self.SERVER_PORT), default_noreply= True, encoding='utf8')
        result = client.get(key)
        client.close()
        if not result:
            return ""
        return result.decode('utf-8-sig')

