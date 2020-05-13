# How to find the information
## General Information 
### Types of container 
Each application log contains generaly more than one container. 
For each application log, the container marked _001 is driver related (i.e where task are distributed to the other containers). The other containers hold the task executor. 

### Driver's container
The driver container main's goals are:  (1) Load the data and (2) distrubute tasks among the executors (which logs are in different containers). 
The driver's containers holds a `TaskManager` and an `ApplicationMaster` (other actors come along, and will be detailed when we explain how to categorize errors). The main idea is that the `TaskManager` will distribute tasks among the executors and the `ApplicationMaster` is in charge of running the app at the driver level. 

### Executor container 
The executor is in charge of runnning tasks given by the driver. When error arise, the executor is often the one to return the error. 

### Errors in general 
Logs contain many errors, however, they are not all relevant to categorize each errors. For instance, many times we can find the error message **ERROR TransportRequestHandler**, but this error is subsequent to a driver related error. 
In fact, as in stacktraces, the first error raised in one of the container is the most relevant one. So we will consider the first errors raised in any containers as the most relevant. This is indeed quiet logical: the first error raised in a container most certainly triggers all subsqeuent error messages. 

Also, we will start by looking at errors raised in the driver's container, and then if these errors are not sufficient we will look at the executor container's errors. 

In summury, we have the two rules: 
1. Look first for errors in the driver's container  
2. Look at the first error raised in the container (appart if its an **ERROR Utils**)

Exhaustive list of possible errors:
More frequent:
ERROR ApplicationMaster
ERROR TaskSetMaster
ERROR YarnClusterScheduler 
ERROR Executor 
ERROR Utils 
(maybe I forgot some, but they are not important if forgotten)

Less frequent: 
ERROR AsyncEventQueue: => triggered after other errors => triggered when the driver crashed due to some other error
ERROR SparkUncaughtExceptionHandler: => again triggered when executor go south due to other error (Type 5 or 6)
ERROR ResourceLeakDetector: 
ERROR TransportResponseHandler: => Driver goes south 
ERROR TransportRequestHandler: => Driver goes south 
ERROR TaskResultGetter: => Driver goes south 
ERROR TransportClient: => to DO
ERROR OneForOneBlockFetcher: => TO DO 
ERROR BlockManagerSlaveEndpoint: => TO DO
ERROR ShutdownHookManager: => ignored 
ERROR ShortCircuitCache: => in executor & then exectuor fails to communication with driver 
ERROR RetryingBlockFetcher: => usually not first error and before a leak detector error  
ERROR TransportChannelHandler: => 
ERROR DFSClient: => with prelaunch.err. 


NB: Error Utils are messages sent that can be viewed as "further information" => chech if they have the exception type in them. 

## Error Category 
### Error type 1: Error starting the Spark job
TO DO

### Error type 2: File error reading input (data) files at the driver and/or executors
This type of error is raised when the driver (or executor) failes to read a file. Its quiet a simple error to detect, as it can be  found if in a single message: **[Error file: prelaunch.err.**. 

Here is an example: 
[2020-04-05 16:40:36.283]Container exited with a non-zero exit code 56. Error file: prelaunch.err.

### Error type 3: Error while executing general (non-spark related) java/scala code at the driver
This is an error that happens in the driver, so we will only look at the driver's container. What will happen mainly is that at some point (maybe after or before the `TaskManager` distributed tasks among the executors) the driver will try to run some java or scala code and it will fail. 
As mentioned earlier, the actor responsible for running such code (or at least monitoring that the ran in the driver runs smoothly) is the `ApplicationMaster`. Hence, for type 3 errors, the first error message should come from the `ApplicationMaster` (see exemple below for a more visual understanding). 
What is intersting with this type of errors is that we can quiet easly find in the stacktrace following the error message, the exception type and line where the app crash (again see exemple for a more visual understanding). 

Naturally, after an error of type 3, the driver will crash and the executors that might have been lauched by the `TaskSetManager` will fail to communicate with the driver. Hence, in the exuctors, usually one can find many warning stating that it failed to communicate with the driver. These executors will finally crash, with an **ERROR Exector** message stating that it failed communicating with the driver. However, all this part about the exector can be ignored as type 3 error are related to the driver hence a single look at the driver's container gives us sufficient infomation about the error. 

Exception + line info in the stacktrace following the **ERROR ApplicationMaster: (...)** message. 

Here is an example:
20/04/07 15:57:11 ERROR ApplicationMaster: User class threw exception: java.lang.OutOfMemoryError: GC overhead limit exceeded
java.lang.OutOfMemoryError: GC overhead limit exceeded
at java.lang.Integer.valueOf(Integer.java:832)
at scala.runtime.BoxesRunTime.boxToInteger(BoxesRunTime.java:65)
at App3$$anonfun$4$$anonfun$apply$2.apply(App3.scala:22)
at App3$$anonfun$4$$anonfun$apply$2.apply(App3.scala:22)
at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
at scala.collection.TraversableLike$$anonfun$map$1.apply(TraversableLike.scala:234)
at scala.collection.immutable.Range.foreach(Range.scala:160)
at scala.collection.TraversableLike$class.map(TraversableLike.scala:234)
at scala.collection.AbstractTraversable.map(Traversable.scala:104)
at App3$$anonfun$4.apply(App3.scala:22)
at App3$$anonfun$4.apply(App3.scala:22)
at scala.collection.TraversableLike$$anonfun$flatMap$1.apply(TraversableLike.scala:241)
at scala.collection.TraversableLike$$anonfun$flatMap$1.apply(TraversableLike.scala:241)
at scala.collection.IndexedSeqOptimized$class.foreach(IndexedSeqOptimized.scala:33)
at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:186)
at scala.collection.TraversableLike$class.flatMap(TraversableLike.scala:241)
at scala.collection.mutable.ArrayOps$ofRef.flatMap(ArrayOps.scala:186)
at App3$.main(App3.scala:22)
at App3.main(App3.scala)
at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
at java.lang.reflect.Method.invoke(Method.java:498)
at org.apache.spark.deploy.yarn.ApplicationMaster$$anon$4.run(ApplicationMaster.scala:721)


### Error Type 4: Error while transferring data between driver and executors.
Type 4 error is related to errors that happen when the driver and the executor can't communicate. This type of error can come in many flavours as it is a quiet general error and many actors can go south before causing an error in the transfer of data between driver and executor. 
We will now try to list an exhaustive lists of errors that can happen: 
1. The first error message in the driver contains the word **driver**
2. The driver goes quiet, doest return any error message, but executor does and has an **ERROR Executor** message that contains the word **driver**
3. Error related to **Error Task(...)**
4. **Error ResourceLeakDetector: (...)**: due to a Netty error (i.e a protocol for communication between clusters) => the driver crashes and cannot communicate with the driver ) (exception is in **ERROR Utils:** (if any and need to be before the ERROR message) then stack trace  of **ERROR RetryingBlockFetcher: (...)** and line have to check the last TaskSetManager line)

TO BE VERIFIED: ERROR TaskSetManager related => type 4 error

Exception can be found in **ERROR Utills:** messages (if any), then in **ERROR ApplicationMaster:** error message (if any), else look in the executors for **ERROR Executor:** stacktrace. 

### Error Type 5 & 6: 
Type 5 and 6 errors are quiet similat. One regards errors running java/scala code at the exector level and the other regards regards either errors while shuffling data (i.e moving data between executors) or while reading data. 
The fact that they are both at the executor level will result in error messages in the driver's container that are very similar. 
The first error message reported at the driver's cluster level should be  **ERROR YarnClusterScheduler: Lost executor  ...**. This error is raised by the `YarnClusterScheduler`, so a bit of background is appropriate to understand why this error is important. 

The `YarnClusterScheduler` is a specific instance of a `TaskScheduler` which responsability is to (1) assign sets of task to the different clusters and (2) keep tracks of the executors in a Spark application (the reason why it is called `YarnClusterScheduler` is because its a `TaskScheduler` specifically designed for a Yarn deployment). The `YarnClusterScheduler` is used (as lock) by the `TaskSetMaster` that returns many INFO messages about which cluster is responsible for runnning which taks. 
The main idea is that when an error message is returned by the `YarnClusterScheduler`, then there must be an error in one of the executor. The question is now, is it an error  because the driver failed to communication with the exectuor or is it because the executor crashed due to some operations at its level ? 
The question can be answered quiet easily: if the first error was returned by `TaskSetMaster` or any kind of Task actor (i.e in the name of the actor there is Task), then it means that the driver lost connection with its executor due to an unrelated code or shuffle data problem (i.e its a type 4 error where the driver and the executor fail to communicate) However, if the `YarnClusterScheduler` first returned the error (which reports error happening in the executor), then this means that the executor reported an arror 
Henceforth, we now assume that if we first have an **ERROR YarnClusterScheduler: Lost executor 2** then we have a type 5 or 6 error. 

Also, another message intersting to look at (but not necessarly important) is: **INFO DAGScheduler: Shuffle files lost ...** => there was a problem while shuffling data. 

Here is an exemple at the driver level: 
20/04/07 16:01:58 ERROR YarnClusterScheduler: Lost executor 1 on iccluster057.iccluster.epfl.ch: Container marked as failed: container_e02_1580812675067_5024_01_000002 on host: iccluster057.iccluster.epfl.ch. Exit status: 143. Diagnostics: [2020-04-07 16:01:58.105]Container killed on request. Exit code is 143

One can see in this message that we can get the name of the cluster that failed. The next step is then to check the exutor cluster that failed and look for the error message **ERROR Executor: (...)**. If in the stacktrace following the latter message we will see the exception raised and from which line the error comes from (i.e at java.(...)). If the first line points to a line of the App (eg. at App7:$$(...)) then its a Type 5 error, otherwise its a type 6 error. 

Exception + line info in the stack trace following the **ERROR Executor: (...)** message. 

### Error type 7: 
Some exception put in type 4 will go there. TO BE DONE 

### Error type 8: 
**ERROR ShortCircuitCache: (...)**: due to an HDFS error => allways in the executor's container

### Error type 9: 




