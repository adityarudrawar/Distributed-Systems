## Name: Aditya Arun Rudrawar
## Assignment: Distributed KV Store

## HOW TO RUN
1. Import the kvstore file from the library
2. You can start the program by initializing the class like `kvs = kvstore.KVStore()`
3. Params that you can pass to KVStore 
```
    consistency : string,
    replicas : int = 3,
    storage_directory : string = '',
    output_directory : string = ''
```
4. After you have initialized the object of the kvstore class. You can start the controlets and datalets by calling the `start()` method on the class
5. You can get the addresses of all the controlets get_controlet_address() method.
6. Clients can connect through the standard pymemcache client.
```
    from pymemcache.client.base import Client
   
   
    c = Client(address)
    response = c.set(key, value, noreply=False)
    print("SET response ", response)
    c.close()
```
7. You can pass the respective string for the consistency you want.
EVENTUAL_CONSISTENCY = 'eventual_consistency'
LINEARIZABLE_CONSISTENCY = 'linearizable_consistency'
SEQUENTIAL_CONSISTENCY = 'sequential_consistency'

8. You can see any of the test files to get an idea. Everything is set in env

## CONSISTENCIES IMPLEMENTED:
1. Linear
2. Sequential
3. Eventual

## ARCHITECTURE:

The architecture was influenced by the [BESPOKV](https://dl.acm.org/doi/abs/10.1109/SC.2018.00005?download=true). The system is divided in to a Control plane and Data plane. The Control plane handles all the communications from the client, broadcasting, ordering from other replicas, and communication between its Datalet layer.

This separation creates nice hot swappable parts. You can simply swap any off the shelf key value store with memcache support for the datalayer.


### LINEAR CONSISTENCY:
#### Design	
![Linear consistency](https://user-images.githubusercontent.com/30310911/235394433-09fdf55a-ab44-47c6-affc-0824b2ecf5e4.png)

#### Implementation
The architecture consists of two main components, the controlet and datalet. The controlet is responsible for managing the metadata for the system, such as the address of the datalets and the current state of the system. The datalet, on the other hand, is responsible for managing the actual key-value data and processing read and write operations. To ensure linear consistency, the system uses total order broadcast with Lamport clocks to order all operations in the system. Each operation is assigned a unique ID, which contains the Lamport clock value of the node that initiated the operation. The ID is included in the broadcast messages, which ensures that all nodes receive the same messages in the same order. 

#### Algorithm:
1. When a request comes to the controlet, it increments its counter, generates a unique id for that operation.
2. The controlet now puts the request with its unique id in its local queue, with its lamport clock. The id is used to get to the status of the request in future operations. Then, the controller broadcasts the request to every other controlet with the unique id.
3. When a controlet receives a broadcast, the broadcast message consists of the unique id, the lamport clock, and then the request is added to its own local queue.
4. When a request is at the top of the queue, the acknowledgment is sent.
5. For a request that is at the top of the queue, and all the acknowledgements are received then the controlet broadcasts a “Ready” message to all the other servers.
6. When a “READY”  msg is received for a request at the top of the queue, the request is popped from the queue and sent to the datalet.

Note: Whenever a request is added to the queue, the queue is sorted based on the lamport clocks.

This TOB is implemented for each operation such as GET/SET for linear consistency.

#### Testing
The test cases are designed to match a real world scenario, the get and set are shuffled and then targeted at the system. This leads to for surety that the system can handle concurrent requests thrown at multiple servers as well, and should have linearizable history.

Run the file: “test_lc.py” for testing linearizability.
Test 1: Multiple SET requests
Test 2: Multiple GET requests
Test 3: Multiple GET, SET requests

In linear consistency, the write and read operations need to be ordered. Therefore, the system uses the total order broadcast 2 Ack method with Lamport clocks for ordering.
When a request is made, the GET/SET requests are broadcasted and sent to datelat in the same sequence.

The sequence of operations that each server performs is stored in the output/linear_consistency_output folder. Each file stores the order of requests it has processed, and that's how we test if every server is processing in the same order.
The system is tested against 3 replicas, a total of 15 get and set requests. The order of operation that resulted in shown below:

The format of the statement is 
`reqid: {unique_request_id} {set/get} key {key}  {response from datalet}`

As you can see all the requests are processed in the same order across all the servers.
Server 0:
Server 1:
Server 2:
Linear Consistency test case output:


### SEQUENTIAL CONSISTENCY:
#### Design
![sequential consistency](https://user-images.githubusercontent.com/30310911/235394449-d19042a4-ca32-4c4c-99d6-87afb7ce96db.png)

#### Implementation:
To implement sequential consistency in the distributed key-value store, a combination of techniques was used. For GET operations, a local read protocol was implemented. This allowed nodes to read data locally without having to go through a consensus protocol, improving the performance and reducing the latency of read operations in the system. When a node receives a read request, it first checks its local copy of the data and returns it if it is up-to-date. Since the local read protocol guarantees that the data is up-to-date, there is no need to perform a consensus protocol for get operations.

For SET operations, Total Order Broadcast (TOB) was used to ensure that all nodes in the system receive messages in the same order. Each message is assigned a unique ID using a combination of the sender's Lamport clock value and a unique identifier for that node. When a node receives a message, it adds it to its incoming message queue and checks if all messages with smaller IDs have been delivered. If so, it adds the message to its deliverable queue and delivers all messages in the queue in the order of their IDs. This technique ensures that all nodes apply messages in the same order, providing linear consistency for the system. By using TOB for set operations, the performance of write operations is improved as they don't require waiting for consensus to be reached before committing the changes.

#### ALGORITHM
1. SET is the same as linear
2. GET is simple read from the datalet

#### TESTING
In sequential consistency, the reads are not ordered but the write needs to be ordered. So, the writes are TOB across the servers and they are sent to the datalets in a sequential order. 
I have logged all the write operations, and tested if they are operated in the same sequences.

Run the test file: “test+sc.py” for sequential consistency.
Test 1: Multiple SET requests
Test 2: Multiple GET requests
Test 3: Multiple GET, SET requests

This was tested with 3 replicas and a total of 15 GET/SET requests in which 7/8 are SET requests.

This is order of all the write requests on all the servers:
Server 0:

### EVENTUAL CONSISTENCY:
#### Design
![Eventual consistency](https://user-images.githubusercontent.com/30310911/235394456-72af64bc-2daa-46f5-ae36-ab21efd9fb66.png)

#### Implementation
To achieve eventual consistency in the distributed key-value store, a local write protocol was implemented. This protocol is used to ensure that all write operations performed on the system are eventually propagated to all datalets, thereby ensuring that all clients have consistent access to the most up-to-date version of the data.

In this protocol, each key is assigned to one of the controlets, which are responsible for all write operations related to that key. The controlets maintain a counter for each key that is updated for every write operation. This counter ensures that there are no race conditions when multiple write operations are performed concurrently.

When a client sends a write request for a key to a specific controlet, that controlet first calculates the hash of the key. The hash function is designed in a way that ensures that the hash value for each key is unique and evenly distributed across all available controlets. Based on the hash value, the controlet then determines which datalet is responsible for storing the data for that key.

If the client sends a request for a key to a controlet that is not responsible for that key, the controlet forwards the request to the correct controlet. This forwarding process ensures that the write request is always directed to the correct controlet responsible for the data for that key. 

When a write request is made to a controlet, it makes sure it is responbsile for that key otherwise forwards to the correct controlet. 

The responsible controlet then forwards the request to its respective datalet. The datalet first checks if the incoming flag (which corresponds to the counter of the request) is higher than the current flag for that key. If the incoming flag is higher, it means that the incoming request is a more recent update than the current version, and the datalet updates its local copy of the data.

Once the datalet has updated its local copy of the data, the respective controlet sends acknowledgement to the client and then broadcasts the write request to all other datalets in the system with its own counter as flag. The same comparison logic is applied at every datalet. This ensures that the most recent update is propagated to all datalets and that eventually all datalets will have the most up-to-date version of the data.
This leads to faster responses to clients and eventual consistencies.
The local write protocol ensures eventual consistency in the system, as there may be a delay in propagating updates to all datalets in the system, resulting in temporary inconsistencies. However, the protocol guarantees that all updates will eventually be propagated to all datalets, thereby ensuring that all clients have consistent access to the most up-to-date version of the data.

For read operation the controlet, just sends the request directly to the datalet and the response back to the client directly.

#### ALGORITHM
1. SET is responded directly back to the client and has an async broadcast, 
2. GET is simple read from the datalet

#### TESTING
Exhibiting stale reads, the test case is in the test_ec.py file.

### MAIN CHALLENGES
1. Deciding on the architecture for the consistencies.
2. Deciding on the implementation of each consistency.
3. Handling the race conditions while implementing the Total Order Broadcast.

### PERFORMANCE TESTING:
The system was set to three replicas and is tested against 100 SET requests at a time, 100 GET requests at a time and 100 SET, GET requests at a time.

### CONSTRAINTS, LIMITATIONS AND ASSUMPTIONS
1. Number of servers is always greater than or equal to 3.
2. If the number of requests is too high, there are unexpected results.
3. The value cannot be longer than 4000 bytes.

### REFERENCES
1. BESPOKV 
2. Slides from the professor
