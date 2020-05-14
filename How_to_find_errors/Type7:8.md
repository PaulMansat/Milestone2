The error presented here might never be tested and they will be the last tested (its a bit like the last chance)
### Error Type 7
This error regards if a spark operation failed at the driver's level only. Because most of the ERROR message will have been previously analysed, in this type of error we are looking for any kind of usefull information in the Driver's container ((i.e the container that has the form {app_id}_{attemptNb}_0001)). Look for any ERROR message (I doubt that you'll find any because we cover all of them in previous part), however, if one is found look if it followed by a stacktrace and try to find the exception type and a line number (in the form of for instance App7.scala:19). If an error is found, but no exception type found then say that the exception type is org.apache.spark.SparkException (the most general error). 

The next step is to check that in the driver contains no ERROR message. If so, then it means it will be a type 8 error. 
If no error message was found, then conclude that its a type 7 error. 
I belive that most of the time we will end with a type 7 error with exception type org.apache.spark.SparkException and line number -1. 


### Error Type 8
Look for an error in the executors' containers. Again, to find the exception look at error's in the executor and check if they have a stack trace (line number and exception type should be there if any). If no exception or line number is found in the executor's container, look in the driver's container. Finnaly, if nothing is found, output the exception org.apache.spark.SparkException and line -1. 

The main difference between type 8 and 7 error, is that in type 7 error, no error message should be found in the executor's container, while for type 8 error's an error message should be found in the execurtor, and we might find one in the driver (but not necessarely). 

