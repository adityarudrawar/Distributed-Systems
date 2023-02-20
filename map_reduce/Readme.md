1. Master Node
2. Mapper ( Process )
3. Groupby ()
4. Reducer ( Process )
5. Logging ( Handled by main process to keep track of no of reducers and mappers, stage of each Mappers, failing conditions, stage of each reducers )

Master will have a port number and each mapper will use that port number to send a message to master.

Mapper and Reducer should know the port number of each other, we can send that data during initiliazation of each reducer/mapper

The Mapper needs to finish their process, before starting the reducers

6. Master node will initialize the mapper process.

7. Master will pass the file/data the mapper needs to process. We can send the file name or number line or we can split the input across multiple files using a combination of number line and some maths we can use the key value store to store the data and that key is then send to the mapper, the mapper reads from the key value store. 

8. Each mapper will have a unique hash code, helping us to keep track of that process.

9. Each mapper will send acknowledgment to Master Node, for either completions of the map functions or if the machine(mapper) failed.

10. The mapper can send a heartbeat/polling message to the master node at a frequent time, and if no heartbeat message is not recieved then the master can assume that mapper is dead and the master can spawn a new mapper (process).

10. If any of the mapper fails then the master will nulify any previous acknowledgement and then create a new mapper as a process ( and assign a new unique id to it ).

11. After the acknowledgment is recieved from every mapper then the master node will kill the mapper processes and then start the Groupby process.

12. The group by    