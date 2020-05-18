import java.io.{BufferedWriter, File, FileWriter}
import java.util.Scanner

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

// Various data structures for simplifying understanding of the code
case class LineData(applicationId: String,
                    attemptNb: Int,
                    exitCode: Int,
                    user: String,
                    date: String,
                    isLaunch: Boolean,
                    exitStatus: String,
                    container: (Int, String))

case class SimpleApplication(applicationId: String, attemptCount: Int, user: String)

case class Attempt(applicationId: String,
                   attemptNumber: Int,
                   startTime: String,
                   endTime: String,
                   finalStatus: String,
                   exitCode: Int,
                   containers: List[(Int, String)])

case class ErrorAttempt(errorCategory: Int, exception: String, stage: Int, sourceCodeLine: Int) {
  override def toString: String = s"""errorCategory: $errorCategory, exception $exception, stage $stage, sourceCodeLine $sourceCodeLine"""
}

case class FullAttemptWithErrors(appId: String,
                                 attemptNumber: Int,
                                 user: String,
                                 startTime: String,
                                 endTime: String,
                                 containers: List[(Int, String)],
                                 errorCategory: Int,
                                 exception: String,
                                 stage: Int,
                                 sourceCodeLine: Int
                                ) {
  override def toString: String = {
    s"""
       |\nAppAttempt  : appattempt_1580812675067_${appId}_${"%06d".format(attemptNumber)}
       |
         |User          : $user
       |
         |StartTime     : $startTime
       |
         |EndTime       : $endTime
       |
         |Containers    : ${containers.map(c => s"container_e02_1580812675067_${appId}_${"%02d".format(attemptNumber)}_${"%06d".format(c._1)} -> ${c._2}").mkString(", ")}
       |
         |ErrorCategory : $errorCategory
       |
         |Exception     : $exception
       |
         |Stage         : $stage
       |
         |SourceCodeLine: $sourceCodeLine """.stripMargin
  }
}

case class Application(applicationId: String,
                       user: String,
                       numAttemps: Int,
                       attempts: List[Attempt]) {

  // For displaying final results formatted
  override def toString: String = {
    val appString =
      s"""ApplicationId : application_1580812675067_$applicationId
         |
         |User          : $user
         |
         |NumAttempts   : $numAttemps""".stripMargin
    appString + attempts.map(a =>
      s"""
         |\nAttemptNumber : ${a.attemptNumber}
         |
         |StartTime     : ${a.startTime}
         |
         |EndTime       : ${a.endTime}
         |
         |FinalStatus   : ${a.finalStatus}
         |
         |Containers    : ${a.containers.map(c => s"(${c._1},${c._2})").mkString(", ")}""".stripMargin).mkString + "\n\n"
    //|
    //|ErrorCategory : ${a.errorCategory}
    //|
    //|Exception     : ${a.exception}
    //|
    //|Stage         : ${a.stage}
    //|
    //|SourceCodeLine : ${a.sourceCodeLine} """.stripMargin).mkString + "\n\n"
  }
}

object Milestone3 {

  def main(args: Array[String]): Unit = {
    // Remove unwanted spark logs
    /*Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)*/

    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    // Delimiter for the aggregated application logs
    val delimiter = "\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*\\*" + System.lineSeparator()

    // Patterns declaration for parsing the data we're interested by or filtering it
    val datePattern = "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})".r
    //"(\\d{2}\\d{2}\\d{2} \\d{2}:\\d{2}:\\d{2})".r => this one is for aggregated logs date pattern!
    val userPattern = ".*user: ([a-z]*).*".r
    val launchPattern = "^.*from [A-Z]* to LAUNCHED.*$"
    val finishPattern = ".*State change from [A-Z_]* to (FINISHING|FAILED|KILLED).*".r
    val appRegex = ".*application_1580812675067_\\d*.*|.*appattempt_1580812675067_\\d*.*|.*container_e02_1580812675067_\\d*.*"
    val appNumberPattern = ".*(application_1580812675067_|appattempt_1580812675067_|container_e02_1580812675067_)(\\d*).*".r
    val attemptNbPattern = ".*(appattempt_1580812675067_\\d*_|container_e02_1580812675067_\\d*_)(\\d*).*".r
    val containerPattern = ".*container_e02_1580812675067_\\d*_\\d*_(\\d*).*(iccluster\\d*\\.iccluster\\.epfl\\.ch).*".r
    val appLogContainerPattern = "Container: container_e02_1580812675067_(\\d*)_\\d*_\\d* on iccluster\\d*\\.iccluster\\.epfl\\.ch.*".r
    val exitStatusPattern = ".* with final state: .*, and exit status: (-*\\d*)".r

    /////////////////////////
    val fullLogFile = args(0)
    val aggregatedLogFile = args(1)
    val startId = Integer.valueOf(args(2))
    val endId = Integer.valueOf(args(3))
    /////////////////////////

    // Format and extract useful LineData out of the entire log file
    // /!\ Necessary work-around, as delimiter can't be set on a per-file basis, so manual split
    // of the full log file needed
    val logsFormattedPlain = sc.textFile(fullLogFile)
    val logsFormatted = logsFormattedPlain.map(_.split("\n")).flatMap(x => x)
      // Filter for lines with date information
      .filter(_.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3} INFO  .*$"))
      // Filter or lines with application information
      .filter(_.matches(appRegex))
      // Extract various information (if present): app number, attempt number, user, finished status, container (using regexes)
      // and map them to a LineData object for easier handling
      .map(line => {
      val appNumberPattern(_, res) = line
      val attemptNb: Int = line match {
        case attemptNbPattern(_, nb) => Integer.valueOf(nb)
        case _ => 0
      }
      val user: String = line match {
        case userPattern(user) => user
        case _ => ""
      }
      val finish: String = line match {
        case finishPattern(finish) => finish
        case _ => ""
      }
      val container: (Int, String) = line match {
        case containerPattern(id, cluster) => (Integer.valueOf(id), cluster)
        case _ => null
      }
      val exitCode: Int = line match {
        case exitStatusPattern(exitCode) => exitCode.toInt
        case _ => 0
      }

      LineData(
        res,
        attemptNb,
        exitCode,
        user,
        datePattern.findFirstIn(line).get,
        line.matches(launchPattern),
        finish,
        container)
    })
      // Filter for useful LineData, removing redundant information
      .filter {
      case LineData(_, _, 0, "", _, false, "", null) => false
      case _ => true
    }
      // Only retain the applications with ids between the provided interval endpoints
      .filter(line => {
      val appId = Integer.valueOf(line.applicationId)

      appId >= startId && appId <= endId
    })
      // Persist the result as it will be used multiple times
      //.persist()

    // Get a map of applicationId => all Attempts made on that ID, sorted by attempt number
    val attempts = logsFormatted
      .filter(line => line.attemptNb > 0)
      // Group lines by application ID and attempt number to enable aggregate on each attempt number
      .groupBy(line => (line.applicationId, line.attemptNb))
      // seqOptAttempts gets all the useful information from all the lines with a given attempt number,
      // then listJoiner simply join every list of attempt returned
      .aggregate[List[Attempt]](Nil)(seqOpAttempts, listJoiner)

      // Filter attempts by states == FAILED OR exit code != 0
      .filter(attempt => attempt.exitCode != 0 || attempt.finalStatus == "FAILED")

      // Sorted by attempt number as requested
      .map(a => ((a.applicationId.toInt, a.attemptNumber), a))
      .toMap

    val appIdToUser = logsFormatted
      .filter(line => line.user.nonEmpty)
      .map(line => (line.applicationId, line.user))
      .collect().toMap

    // RDD of the form (appId -> Array of logs of each containers)
    // Note: key is only None for the end of file (that just contains blank space)
    //TODO : CONVERT THIS BACK TO RDD[Iterator[String]]
  val aggregatedFailedApps = sc.textFile(aggregatedLogFile)
      .map(x => {
      val appId = appLogContainerPattern.findFirstMatchIn(x)
      val attemptNumber = attemptNbPattern.findFirstMatchIn(x)

      (appId, attemptNumber, x)
    }).filter(_._1.isDefined)
      .map(x => ((x._1.get.group(1).toInt, x._2.get.group(2).toInt), x._3))


      // Only retain the applications with ids between the provided interval endpoints
      .filter(x => x._1._1 >= startId && x._1._1 <= endId && attempts.contains(x._1))
      .map(x => (attempts(x._1), x._2))
      .groupByKey()
      .persist()

    val results = aggregatedFailedApps.map(x => (x._1, f1(x._2))).map {
      case (
        Attempt(appId, attemptNumber, startTime, endTime, _, _, containers),
        ErrorAttempt(errorCategory, exception, stage, sourceCodeLine)) =>
        FullAttemptWithErrors(
          appId,
          attemptNumber,
          appIdToUser(appId),
          startTime,
          endTime,
          containers,
          errorCategory,
          exception,
          stage,
          sourceCodeLine
        )
    }

    /*
    // Start by checking all the application Ids from the logs
    // in [startId, endId]
    val allIds = logsFormattedPlain.map(_.split("\n")).flatMap(x => x).map(x => {
      val idPattern = "application_1580812675067_(\\d*)".r

      idPattern.findAllMatchIn(x)
    }).flatMap(x => x).map(x => x.group(1).toInt).filter(x => (x >= startId) && (x <= endId)).collect().toSet

    // Check all application Ids in [startId, endId]
    // that appear in the aggregated logs
    val logsIds = sc.textFile(aggregatedLogFile).map(appLogContainerPattern.findFirstMatchIn(_))
      .filter(_.isDefined)
      .map(_.get.group(1).toInt)
      .filter(x => x >= startId && x <= endId).collect().toSet

    // Take the set difference of both, to have only the app Ids
    // that don't have container logs
    val missingIds = allIds -- logsIds

    // Finally, find the errors that made them fail
    // and categorize them in category 1
    val category1NoLogs = logsFormattedPlain.map(x => {
      val error = ".* INFO Client: Deleted staging directory .*iccluster\\d*\\.iccluster\\.epfl\\.ch" +
        ".*application_1580812675067_(\\d*)\nException in thread \"main\" (.*): .*"
      val errorPattern = error.r

      errorPattern.findAllMatchIn(x)
    }).flatMap(x => x).map(x => (x.group(1).toInt, x.group(2))).filter(x => missingIds.contains(x._1)).map(x => (x._1, ErrorAttempt(1, x._2, -1, -1)))
    */

    val file = "answers.txt"
    val writer = new BufferedWriter(new FileWriter(file))

    results.collect().foreach(a => writer.write(a.toString()))

    writer.close()
  }

  /** Check if the log corresponds to a type 1 error
    *
    * @param lines an iterable[String], where each element is the log of a container for failed attempts
    * @return ErrorAttempt if it's a type 1 error, otherwise call error function 2 (i.e f2)
    */
  def f1(lines: Iterable[String]): ErrorAttempt = {
    // From the forum, only case where this creates logs is for incorrect class name
    // The other have to be found in the regular hadoop logs
    val errorPattern = "(\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2}) ERROR ApplicationMaster: Uncaught exception: \njava.lang.ClassNotFoundException: ".r
    val containerPattern = "Container: container_e02_1580812675067_\\d*_\\d*_000001 on iccluster\\d*\\.iccluster\\.epfl\\.ch.*".r

    val missingClass = lines.filter(x => containerPattern.findFirstMatchIn(x).isDefined).map(x => {
      val errorMatch = errorPattern.findFirstMatchIn(x)

      if (errorMatch.isDefined)
        ErrorAttempt(1, "java.lang.ClassNotFoundException", -1, -1)
      else
        null
    }).headOption

    // If ErrorAttempt is well defined, return the result
    // Otherwise, it means that all checks are not passed. Then call next error function 2 (i.e f2)
    val res = missingClass.orNull
    if (res == null) {
      f2(lines)
    } else {
      res
    }
  }


  /** Check if the log corresponds to a type 2 error
    *
    * @param lines an iterable[String], where each element is the log of a container for failed attempts
    * @return ErrorAttempt if it's a type 2 error, otherwise call error function 3 (i.e f3)
    */
  def f2(lines: Iterable[String]): ErrorAttempt = {
    // Only case specified in forum for this is for missing data file

    val error = "(\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2}) ERROR ApplicationMaster: User class threw exception: org\\.apache\\.hadoop\\.mapred\\.InvalidInputException:" +
      "([\\s\\S]*)?(?=\\n.*?=|\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2})"
    val errorPattern = error.r
    val containerPattern = "Container: container_e02_1580812675067_\\d*_\\d*_000001 on iccluster\\d*\\.iccluster\\.epfl\\.ch.*".r

    val missingFile = lines.map(x => {
      val errorMatch = errorPattern.findFirstMatchIn(x)

      if (errorMatch.isDefined) {
        val errorLinePattern = "at .*\\(App.*:(.*)\\)".r

        val stackTrace = errorMatch.get.group(2)

        val errorLineMatch = errorLinePattern.findFirstMatchIn(stackTrace)

        var errorLine = -1

        if (errorLineMatch.isDefined)
          errorLine = errorLineMatch.get.group(1).toInt

        ErrorAttempt(2, "org.apache.hadoop.mapred.InvalidInputException", -1, errorLine)
      } else
        null
    }).find(_ != null)

    // If ErrorAttempt is well defined, return the result
    // Otherwise, it means that all checks are not passed. Then call next error function 3 (i.e f3)
    val res = missingFile.orNull
    if (res == null) {
      f3(lines)
    } else {
      res
    }
  }

  /** Check if the log corresponds to a type 3 error
    *
    * @param lines an iterable[String], where each element is the log of a container for failed attempts
    * @return ErrorAttempt if it's a type 3 error, otherwise call error function 4 (i.e f4)
    */
  def f3(lines: Iterable[String]): ErrorAttempt = {
    val error = "(\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2}) INFO ApplicationMaster: Final app status: .*, exitCode: \\d*, " +
      "\\(reason: User class threw exception: (.*): .*" +
      "([\\s\\S]*)?(?=\\n.*?=|\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2})"

    val errorPattern = error.r
    val containerPattern = "Container: container_e02_1580812675067_\\d*_\\d*_000001 on iccluster\\d*\\.iccluster\\.epfl\\.ch.*".r
    val stagePattern = "(\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2}) INFO YarnClusterScheduler: Cancelling stage (\\d*)".r

    val drvierError = lines.filter(x => containerPattern.findFirstMatchIn(x).isDefined).map(x => {
      val errorMatch = errorPattern.findFirstMatchIn(x)

      // Stage failure => something happened on the executors
      if (errorMatch.isDefined && !errorMatch.get.group(2).contains("due to stage failure")) {
        val errorLinePattern = "at .*\\(App.*:(.*)\\)".r

        val stackTrace = errorMatch.get.group(3)

        val errorLineMatch = errorLinePattern.findFirstMatchIn(stackTrace)

        var errorLine = -1

        if (errorLineMatch.isDefined)
          errorLine = errorLineMatch.get.group(1).toInt

        val stageMatch = stagePattern.findAllMatchIn(x).toList

        var stage = -1

        if (stageMatch.nonEmpty)
          stage = stageMatch.last.group(2).toInt

        if (errorMatch.isDefined) {
          val errorString = errorMatch.get.group(2)

          if (errorString.indexOf(":") != -1)
            ErrorAttempt(3, errorString.substring(0, errorString.indexOf(":")), stage, errorLine)
          else
            ErrorAttempt(3, errorString, stage, errorLine)
        }
        else
          null
      } else
        null
    }).headOption

    val res = drvierError.orNull

    // If ErrorAttempt is well defined, return the result
    // Otherwise, it means that all checks are not passed. Then call next error function 4 (i.e f4)
    if (res == null) {
      f4(lines)
    } else {
      res
    }
  }

  /** Check if the log corresponds to a type 4 error
    *
    * @param lines an iterable[String], where each element is the log of a container for failed attempts
    * @return ErrorAttempt if it's a type 4 error, otherwise call error function 5 (i.e f5and6)
    */
  def f4(lines: Iterable[String]): ErrorAttempt = {
    // find driver's container
    val containerPattern = "Container: container_e02_1580812675067_\\d*_\\d*_(\\d*) on iccluster\\d*\\.iccluster\\.epfl\\.ch.*".r
    val mapContainers = lines.map(x => {
      (containerPattern.findFirstMatchIn(x).get.group(1).toInt, x)
    }).toMap

    val driverContainer = mapContainers.get(1).get

    // determine the stage and the source line
    var stageLine = (-1, -1)
    val stageLinePattern = ".*INFO DAGScheduler: Final stage: ResultStage (\\d*) .* at App\\d+.scala:(\\d+) *".r
    val stageLineLine = stageLinePattern.findFirstMatchIn(driverContainer)
    if (stageLineLine.isDefined) {
      val s = stageLineLine.get
      stageLine = (s.group(1).toInt, s.group(2).toInt)
    }

    // initialize the potential Error Attempt
    var tempRes = ErrorAttempt(-1, "", -1, -1)

    // I. we start by studying the driver's container for type 4 error message

    // checks that the error does not come from an insufficient space in the driver
    if (driverContainer.contains("than spark.driver.maxResultSize")) {
      tempRes = ErrorAttempt(4, "org.apache.spark.SparkException", stageLine._1, stageLine._2)
    }
    // checks that the error does come from a BlockManager failure
    else if (driverContainer.contains("WARN BlockManager: Failed to fetch")) {
      val warnBlockManagerPattern = ".*WARN BlockManager: Failed to fetch .* (.*Exception|.*Error): .*".r
      val excep2 = driverContainer.replace('\n', ' ') match {
        case warnBlockManagerPattern(e) => e.split(":").take(1)(0)
        case _ => ""
      }
      // search exceptions in INFO/ERROR applicationMaster and ERROR Utils
      // otherwise return exception in the line of the corresponding cases
      tempRes = chooseExcep(driverContainer, excep2, stageLine)
    }
    // checks that the error does come from the TransportChannelHandler
    else if (driverContainer.contains("WARN TransportChannelHandler:")) {
      val TransportChannelHandlerPattern = ".*WARN TransportChannelHandler: .*\n(.*Exception|.*Error): .*".r
      val transportChannelHandelerLine = TransportChannelHandlerPattern.findFirstMatchIn(driverContainer)
      var excep3 = ""
      if (transportChannelHandelerLine.isDefined) {
        excep3 = transportChannelHandelerLine.get.group(1)
      }
      tempRes = chooseExcep(driverContainer, excep3, stageLine)
    }
    // checks that the error comes from either the TaskResultGetter and not from YarnClusterScheduler
    // the YarnClusterScheduler is responsible for keeping track of the executors launched by the driver
    // Hence, if an error due to some code (or due to shuffling data) happen in the executor, the YarnClusterScheduler
    // will mention it, and it will be a type 5 or 6 error (and not type 4)
    else if (driverContainer.contains("ERROR TaskResultGetter") && !driverContainer.contains("ERROR YarnClusterScheduler")) {
      tempRes = chooseExcep(driverContainer, "", stageLine)
    }
    // checks that the error comes from either the TaskResultGetter and not from YarnClusterScheduler
    else if (driverContainer.contains("ERROR ResourceLeakDetector") || driverContainer.contains("ERROR TransportResponseHandler")) {
      tempRes = chooseExcep(driverContainer, "", stageLine)
    }
    // checks that the error comes from ERROR ApplicationMaster and make sure that the error message does
    // not contains ExecutorLostFailure, which is not a type 4 error
    else {
      // ERROR ApplicationMaster without ExecutorLostFailure
      val appMasterPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR ApplicationMaster: (.*)".r
      val appMasterLine = appMasterPattern.findFirstMatchIn(driverContainer)
      if (appMasterLine.isDefined) {
        if (!appMasterLine.get.group(1).contains("ExecutorLostFailure")) {
          val appMasterExceptionPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR ApplicationMaster: User class threw exception: org.apache.spark.SparkException: .*: ([^:]*Exception):*".r
          val appMasterExceptionLine = appMasterExceptionPattern.findFirstMatchIn(driverContainer)
          if (appMasterExceptionLine.isDefined) {
            tempRes = chooseExcep(driverContainer, appMasterExceptionLine.get.group(1), stageLine)
          }
        }
      }
      // checks that the error comes from ERROR TaskSetManager and make sure that the error message does
      // contain "driver"
      val taskSetManagerPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR TaskSetManager: (.*)\n".r
      val taskSetManagerLine = taskSetManagerPattern.findFirstMatchIn(driverContainer)
      if (taskSetManagerLine.isDefined) {
        if (taskSetManagerLine.get.group(1).contains("driver")) {
          tempRes = chooseExcep(driverContainer, "", stageLine)
        }
      }
    }

    // II. If no error messages of type 4 errors were found in the driver's container, look into the executor's container
    val execContainer = mapContainers.filter(x => x._1 > 1)

    if (tempRes.errorCategory == -1) {
      // Go through executor's log and check error cases
      tempRes = execContainer.foldLeft(tempRes)((z, x) => {
        if (z.errorCategory == -1) {
          // fetch the executor's log
          val line = x._2
          // checks that the error comes from ERROR OneForOneBlockFetcher
          if (line.contains("ERROR OneForOneBlockFetcher: Failed while starting block fetches")) {
            val blockFetcherPattern = ".*ERROR OneForOneBlockFetcher: Failed while starting block fetches (.*Exception|.*Error): .*".r
            val excep_ = line.replace('\n', ' ') match {
              case blockFetcherPattern(e) => e.split(":").take(1)(0)
              case _ => ""
            }
            chooseExcep(driverContainer, excep_, stageLine)
          }
          // checks that the error comes from ERROR ShortCircuitCache
          else if (line.contains("ERROR ShortCircuitCache")) {
            val shortCircuitPattern = ".*ERROR ShortCircuitCache:.*\n([^:]*):*".r
            val shortCircuitLine = shortCircuitPattern.findFirstMatchIn(line)
            var excep4 = ""
            if (shortCircuitLine.isDefined) {
              excep4 = shortCircuitLine.get.group(1)
            }
            chooseExcep(driverContainer, excep4, stageLine)
          }
          // checks that the error comes from ERROR Executor and make sure that the error
          // does contain "driver"
          else if (line.contains("ERROR Executor:")) {
            val executorPattern = ".* ERROR Executor: (.*)".r
            val executorLine = executorPattern.findFirstMatchIn(line)
            if (executorLine.isDefined) {
              if (executorLine.get.group(1).contains("driver")) {
                ErrorAttempt(4, "org.apache.spark.SparkException", stageLine._1, stageLine._2)
              // Otherwise, return the original accumulator
              } else {
                z
              }
            // Otherwise, return the original accumulator
            } else {
              z
            }
            // Otherwise, return the original accumulator
          } else {
            z
          }
        // Otherwise, return the original accumulator
        } else {
          z
        }
      })
    }

    // If ErrorAttempt is well defined, return the result
    // Otherwise, it means that all checks are not passed. Then call next error function 5 (i.e f5and6)
    if (tempRes.errorCategory != -1) {
      tempRes
    } else {
      f5and6(lines)
    }

  }

  /** A helper method to find type 4 errors. It studies ERROR Utils messages and INFO/ERROR ApplicationMaster messages
    * to find the appropriate exception
    *
    * @param line      an iterable[String], where each element is the log of a container for failed attempts
    * @param excep     potential exception found with the ERROR Message studied earlier
    * @param stageLine tuple which contains the stage and source code line of the error
    * @return ErrorAttempt if exception found, otherwise null
    */
  def chooseExcep(line: String, excep: String, stageLine: (Int, Int)): ErrorAttempt = {
    var exception = ""
    // Determine the regex pattern for error messages coming from INFO/ERROR ApplicationMaster and ERROR Utils
    val appliMasterPattern = ".*(ERROR|INFO) ApplicationMaster: .* threw exception: (.*Exception):.*".r
    val firstUsefulUtilsMessage = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR Utils: .*\n([^:]*):*".r
    val UtilsExceptionLine = firstUsefulUtilsMessage.findFirstMatchIn(line)
    // When ERROR Utils returns the exception
    if (UtilsExceptionLine.isDefined) {
      exception = UtilsExceptionLine.get.group(1)
    }
    // When ERROR Utils does not return the exception, search for the exception in INFO/ERROR ApplicationMaster
    if (exception.isEmpty) {
      val appliExcep = appliMasterPattern.findFirstMatchIn(line)
      if (appliExcep.isDefined)
        exception = appliExcep.get.group(2)
    }

    // When the excep is not empty, return the corresponding ErrorAttempt with excep as the final exception.
    // Otherwise, use exception found in ERROR Utils or INFO/ERROR ApplicationMaster (if defined)
    if (!excep.isEmpty) {
      ErrorAttempt(4, excep, stageLine._1, stageLine._2)
    } else if (!exception.isEmpty) {
      ErrorAttempt(4, exception, stageLine._1, stageLine._2)
    } else {
      null
    }
  }

  /** Check if the log corresponds to a type 5 or a type 6 error
    *
    * @param lines an iterable[String], where each element is the log of a container for failed attempts
    * @return ErrorAttempt if it's a type 4 error, otherwise call error function 7 (i.e f7)
    */
  def f5and6(lines: Iterable[String]): ErrorAttempt = {
    val ContainerPattern = "Container: container_e02_1580812675067_\\d*_\\d*_(\\d*) on (iccluster\\d*\\.iccluster\\.epfl\\.ch).*".r

    // CHANGE
    val containers = lines.map(x => {
      val containerLine = ContainerPattern.findFirstMatchIn(x)
      (containerLine.get.group(1).toInt, (containerLine.get.group(2), x))
    }).toMap


    val driverContainer = containers.get(1).get._2

    val firstErrorPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR (\\w*):*".r
    val errorDriverPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR YarnClusterScheduler: Lost executor \\d* on iccluster\\d*\\.iccluster\\.epfl\\.ch: Container marked as failed: container_e02_1580812675067_\\d{4}_\\d{2}_(\\d{6})*".r

    val errorActors = firstErrorPattern.findAllMatchIn(driverContainer).map(x => x.group(1))

    // A tuple (Nb of Utils message before actual error, String)
    val firstErrorActor = errorActors.foldLeft((0, 0, ""))(op = (z, x) => {
      if (z._1 == 0) {
        if (x == "Utils") (0, z._2 + 1, "")
        else (1, z._2, x)
      } else {
        z
      }
    })
    var exceptionAndType = ("", -1)
    var stage = -1
    var line = -1

    // analyse the executor's container
    if (firstErrorActor._3 == "YarnClusterScheduler") {
      val executorLineNb = errorDriverPattern.findFirstMatchIn(driverContainer)
      if (executorLineNb.isDefined) {
        val executorNb = executorLineNb.get.group(1).toInt

        val executorErrorPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR Executor: .*\n([^:]*):.*\n.*at (.*)\n".r
        val executorContainer = containers.get(executorNb)
        exceptionAndType = executorContainer.map(x => {
          val executorMatcher = executorErrorPattern.findFirstMatchIn(x._2)
          if (executorMatcher.isDefined) (executorMatcher.get.group(1), if (executorMatcher.get.group(2).contains("App")) 5 else 6)
          else ("", -1) // will never be reached
        }).toList.head


        val lineExecutorPattern = "App\\d*.scala:(\\d*)".r
        line = executorContainer.map(x => {
          val lineMatch = lineExecutorPattern.findFirstMatchIn(x._2)
          if (lineMatch.isDefined) lineMatch.get.group(1).toInt
          else -1
        }).toList.head
      }

      if (exceptionAndType._1 == "") {
        if (firstErrorActor._2 != 0) {
          val firstUsefulUtilsMessage = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} INFO Utils: .*\n([^:]*):*".r
          val UtilsExceptionLine = firstUsefulUtilsMessage.findFirstMatchIn(driverContainer)
          if (UtilsExceptionLine.isDefined)
            exceptionAndType = (UtilsExceptionLine.get.group(1), exceptionAndType._2)
        }
      }

      if (exceptionAndType._1 == "") {
        val ApplicationMasterPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR ApplicationMaster: User class threw exception: ([^:]*):*".r
        val ApplicationMasterLine = ApplicationMasterPattern.findFirstMatchIn(driverContainer)
        if (ApplicationMasterLine.isDefined)
          exceptionAndType = (ApplicationMasterLine.get.group(1), exceptionAndType._2)
      }


      val lineDriverPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} INFO DAGScheduler: .* \\(.* at App\\d*.scala:(\\d*)\\) failed *".r

      if (line == -1) {
        val lineEx = lineDriverPattern.findFirstMatchIn(driverContainer)
        if (lineEx.isDefined)
          line = lineEx.get.group(1).toInt
      }
      //get the stage
      val stagePattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} INFO YarnClusterScheduler: Cancelling stage (\\d*)".r
      val stageLine = stagePattern.findFirstMatchIn(driverContainer)
      if (stageLine.isDefined)
        stage = stageLine.get.group(1).toInt
    }

    // If ErrorAttempt is well defined, return the result
    // Otherwise, it means that all checks are not passed. Then call next error function 7 (i.e f7)
    if (exceptionAndType._1 != "" && exceptionAndType._2 != -1 && stage != -1) {
      //println(exceptionAndType._2 + ", "+ exceptionAndType._1 + ", " + stage + ", " + line)
      ErrorAttempt(exceptionAndType._2, exceptionAndType._1, stage, line)
    } else {
      f7(lines, driverContainer)
    }
  }

  /** Check if the log corresponds to a type 7 error
    *
    * @param lines an iterable[String], where each element is the log of a container for failed attempts
    * @return ErrorAttempt if it's a type 7 error, otherwise call error function 8 (i.e f8)
    */
  def f7(lines: Iterable[String], driverContainerLine: String): ErrorAttempt = {
    val firstErrorPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR".r

    val firstError = firstErrorPattern.findFirstIn(driverContainerLine)

    // If firstError is well defined, return the result ErrorAttempt
    // Otherwise, it means that all checks are not passed. Then call next error function 8 (i.e f8)
    if (firstError.isDefined) {
      genericFindErrorAttempt(7, driverContainerLine)
    } else {
      f8(lines)
    }
  }

  /** Check if the log corresponds to a type 8 error
    *
    * @param lines an iterable[String], where each element is the log of a container for failed attempts
    * @return ErrorAttempt if it's a type 8 error, otherwise call error function 9 (i.e f9)
    */
  def f8(lines: Iterable[String]): ErrorAttempt = {
    val firstErrorPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR".r

    val linesWithErrors = lines.filter(line => firstErrorPattern.findFirstIn(line).isDefined)
    // If linesWithErrors is well defined, return the result ErrorAttempt
    // Otherwise, it means that all checks are not passed. Then call next error function 9 (i.e f9)
    if (linesWithErrors.nonEmpty) {
      genericFindErrorAttempt(8, linesWithErrors.head)
    } else {
      f9(lines)
    }
  }

  def genericFindErrorAttempt(errorCategory: Int, line: String): ErrorAttempt = {
    // Get stage (highest stage information in the log => TODO probably not correct to do that)
    val stagePattern = "stage (\\d)".r
    val stages = stagePattern.findAllMatchIn(line)
    var stage = -1
    if (stages.nonEmpty) {
      stage = stages.map(x => {
        var res = -1
        try {
          res = x.group(1).toInt
        } catch {
          case _: NumberFormatException =>
        }
        res
      }).max
    }

    // Get exception (take the first one available currently and any more specific than org.apache.spark.SparkException)
    val exceptionPattern = "((java\\.[^(\\s|$)]*(Exception|Error)|org\\.apache\\.spark\\.[^(\\s|$)]*(Exception|Error)))".r
    val defaultException = "org.apache.spark.SparkException"
    var exception = defaultException
    val exceptionMatches = exceptionPattern.findAllMatchIn(line)
    if (exceptionMatches.nonEmpty) {
      exception = exceptionMatches.map(_.group(1)).reduce((acc, newVal) => (acc, newVal) match {
        case (`defaultException`, any) => any
        case (any, `defaultException`) => any
        case (any, _) => any
      })
    }

    // Get line (lowest line information in the log)
    val lineExecutorPattern = ("(\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} INFO DAGScheduler: .* \\(.* at App\\d*.scala:(\\d*)\\) failed" +
      "|App\\d\\$.main\\(App\\d*.scala:(\\d+)\\)" +
      "|App\\d.scala:(\\d+)\\) failed)").r
    val lines = lineExecutorPattern.findAllMatchIn(line)
    var lineNumber = Int.MaxValue
    if (lines.nonEmpty) {
      lineNumber = lines.map(x => {
        var res = Int.MaxValue
        try {
          res = x.group(1).toInt
        } catch {
          case _: NumberFormatException =>
        }
        if (res == Int.MaxValue) {
          try {
            res = x.group(2).toInt
          } catch {
            case _: NumberFormatException =>
          }
        }
        res
      }).min
    }
    if (lineNumber == Int.MaxValue) {
      lineNumber = -1
    }

    ErrorAttempt(errorCategory, exception, stage, lineNumber)
  }

  /** Check if the log corresponds to a type 9 error
    *
    * @param lines an iterable[String], where each element is the log of a container for failed attempts
    * @return ErrorAttempt
    */
  def f9(lines: Iterable[String]): ErrorAttempt = {
    ErrorAttempt(9, "N/A", -1, -1)
  }

  def listJoiner[U](l1: List[U], l2: List[U]): List[U] = l1 ++ l2

  def seqOpAttempts(list: List[Attempt], attempt: ((String, Int), Iterable[LineData])): List[Attempt] = {
    def folder(agg: Attempt, line: LineData): Attempt = (agg, line) match {
      case (Attempt(a1, a2, _, endTime, finalStatus, exitCode, containers), LineData(_, _, 0, _, startTime, true, _, _)) =>
        Attempt(a1, a2, startTime, endTime, finalStatus, exitCode, containers)
      case (Attempt(a1, a2, startTime, _, _, exitCode, containers), LineData(_, _, 0, _, endTime, false, finalStatus, null)) =>
        Attempt(a1, a2, startTime, endTime, finalStatus, exitCode, containers)
      case (Attempt(a1, a2, startTime, endTime, finalStatus, exitCode, containers), LineData(_, _, 0, _, _, _, _, null)) =>
        Attempt(a1, a2, startTime, endTime, finalStatus, exitCode, containers)
      case (Attempt(a1, a2, startTime, endTime, finalStatus, _, containers), LineData(_, _, exitCode, _, _, _, _, null)) =>
        Attempt(a1, a2, startTime, endTime, finalStatus, exitCode, containers)
      case (Attempt(a1, a2, startTime, endTime, finalStatus, exitCode, containers), LineData(_, _, _, _, _, _, _, container)) =>
        Attempt(a1, a2, startTime, endTime, finalStatus, exitCode, container :: containers)
    }

    val tempNewAttempt: Attempt = attempt._2.foldLeft[Attempt](Attempt(attempt._1._1, attempt._1._2, "", "", "", 0, Nil))(folder)

    val newAttempt = Attempt(
      tempNewAttempt.applicationId,
      tempNewAttempt.attemptNumber,
      tempNewAttempt.startTime,
      tempNewAttempt.endTime,
      tempNewAttempt.finalStatus,
      tempNewAttempt.exitCode,
      tempNewAttempt.containers.distinct.sortBy(_._1)
    )

    newAttempt :: list
  }
}
