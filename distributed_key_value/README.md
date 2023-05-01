Name: Aditya Arun Rudrawar
Assignment: Distributed KV Store

CONSISTENCIES IMPLEMENTED:
Linear
Sequential
Eventual

LINEAR CONSISTENCY:
Design	
![Linear consistency](https://user-images.githubusercontent.com/30310911/235394433-09fdf55a-ab44-47c6-affc-0824b2ecf5e4.png)

Implementation
The architecture consists of two main components, the controlet and datalet. The controlet is responsible for managing the metadata for the system, such as the address of the datalets and the current state of the system. The datalet, on the other hand, is responsible for managing the actual key-value data and processing read and write operations. To ensure linear consistency, the system uses total order broadcast with Lamport clocks to order all operations in the system. Each operation is assigned a unique ID, which contains the Lamport clock value of the node that initiated the operation. The ID is included in the broadcast messages, which ensures that all nodes receive the same messages in the same order. 

Algorithm:
When a request comes to the controlet, it increments its counter, generates a unique id for that operation.
The controlet now puts the request with its unique id in its local queue, with its lamport clock. The id is used to get to the status of the request in future operations. Then, the controller broadcasts the request to every other controlet with the unique id.
When a controlet receives a broadcast, the broadcast message consists of the unique id, the lamport clock, and then the request is added to its own local queue.
When a request is at the top of the queue, the acknowledgment is sent.
For a request that is at the top of the queue, and all the acknowledgements are received then the controlet broadcasts a “Ready” message to all the other servers.
When a “READY”  msg is received for a request at the top of the queue, the request is popped from the queue and sent to the datalet.

Note: Whenever a request is added to the queue, the queue is sorted based on the lamport clocks.

This TOB is implemented for each operation such as GET/SET for linear consistency.


Performance evaluation


SEQUENTIAL CONSISTENCY:
Design
![sequential consistency](https://user-images.githubusercontent.com/30310911/235394449-d19042a4-ca32-4c4c-99d6-87afb7ce96db.png)

Implementation:
To implement sequential consistency in the distributed key-value store, a combination of techniques was used. For GET operations, a local read protocol was implemented. This allowed nodes to read data locally without having to go through a consensus protocol, improving the performance and reducing the latency of read operations in the system. When a node receives a read request, it first checks its local copy of the data and returns it if it is up-to-date. Since the local read protocol guarantees that the data is up-to-date, there is no need to perform a consensus protocol for get operations.

For SET operations, Total Order Broadcast (TOB) was used to ensure that all nodes in the system receive messages in the same order. Each message is assigned a unique ID using a combination of the sender's Lamport clock value and a unique identifier for that node. When a node receives a message, it adds it to its incoming message queue and checks if all messages with smaller IDs have been delivered. If so, it adds the message to its deliverable queue and delivers all messages in the queue in the order of their IDs. This technique ensures that all nodes apply messages in the same order, providing linear consistency for the system. By using TOB for set operations, the performance of write operations is improved as they don't require waiting for consensus to be reached before committing the changes.

EVENTUAL CONSISTENCY:
Design
![Eventual consistency](https://user-images.githubusercontent.com/30310911/235394456-72af64bc-2daa-46f5-ae36-ab21efd9fb66.png)

Implementation
To achieve eventual consistency in the distributed key-value store, a local write protocol was implemented. This protocol is used to ensure that all write operations performed on the system are eventually propagated to all datalets, thereby ensuring that all clients have consistent access to the most up-to-date version of the data.

In this protocol, each key is assigned to one of the controlets, which are responsible for all write operations related to that key. The controlets maintain a counter for each key that is updated for every write operation. This counter ensures that there are no race conditions when multiple write operations are performed concurrently.

When a client sends a write request for a key to a specific controlet, that controlet first calculates the hash of the key. The hash function is designed in a way that ensures that the hash value for each key is unique and evenly distributed across all available controlets. Based on the hash value, the controlet then determines which datalet is responsible for storing the data for that key.

If the client sends a request for a key to a controlet that is not responsible for that key, the controlet forwards the request to the correct controlet. This forwarding process ensures that the write request is always directed to the correct controlet responsible for the data for that key. 

When a write request is made to a controlet, it makes sure it is respobsile for that key otherwise forwards to the correct controlet. 

The responsible controlet then forwards the request to its respective datalet. The datalet first checks if the incoming flag (which corresponds to the counter of the request) is higher than the current flag for that key. If the incoming flag is higher, it means that the incoming request is a more recent update than the current version, and the datalet updates its local copy of the data.

Once the datalet has updated its local copy of the data, the respective controlet then broadcasts the write request to all other datalets in the system with its own counter as flag. The same comparison logic is applied at every datalet. This ensures that the most recent update is propagated to all datalets and that eventually all datalets will have the most up-to-date version of the data.

The local write protocol ensures eventual consistency in the system, as there may be a delay in propagating updates to all datalets in the system, resulting in temporary inconsistencies. However, the protocol guarantees that all updates will eventually be propagated to all datalets, thereby ensuring that all clients have consistent access to the most up-to-date version of the data.

For read operation the controlet, just sends the request directly to the datalet and the response back to the client directly.


MAIN CHALLENGES
CLIENT API
CONSTRAINTS, LIMITATIONS AND ASSUMPTIONS

REFERENCES:
BESPOKV
