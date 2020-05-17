import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

// Various data structures for simplifying understanding of the code
case class LineData(applicationId: String,
                    attemptNb: Int,
                    user: String,
                    date: String,
                    isLaunch: Boolean,
                    finishStatus: String,
                    container: (Int, String))

case class SimpleApplication(applicationId: String, attemptCount: Int, user: String)

case class Attempt(applicationId: String,
                   attemptNumber: Int,
                   startTime: String,
                   endTime: String,
                   finalStatus: String,
                   containers: List[(Int, String)])

case class ErrorAttempt(errorCategory: Int, exception: String, stage: Int, sourceCodeLine: Int)

case class FullAttemptWithErrors(attemptNumber: Int,
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
       |\nAttemptNumber : ${attemptNumber}
       |
         |User          : ${user}
       |
         |StartTime     : ${startTime}
       |
         |EndTime       : ${endTime}
       |
         |Containers    : ${containers.map(c => s"(${c._1},${c._2})").mkString(", ")}
       |
         |ErrorCategory : ${errorCategory}
       |
         |Exception     : ${exception}
       |
         |Stage         : ${stage}
       |
         |SourceCodeLine: ${sourceCodeLine} """.stripMargin
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
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    // Delimiter for the aggregated application logs
    val delimiter = "***********************************************************************\n"

    sc.hadoopConfiguration.set("textinputformat.record.delimiter", delimiter)

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

      LineData(res,
        attemptNb,
        user,
        datePattern.findFirstIn(line).get,
        line.matches(launchPattern),
        finish,
        container)
    })
      // Filter for useful LineData, removing redundant information
      .filter {
      case LineData(_, _, "", _, false, "", null) => false
      case LineData(_, _, _, _, _, "FINISHING", _) => false
      case LineData(_, _, _, _, _, "KILLED", _) => false
      case _ => true
    }
      // Only retain the applications with ids between the provided interval endpoints
      .filter(line => {
      val appId = Integer.valueOf(line.applicationId)

      appId >= startId && appId <= endId
    })
      // Persist the result as it will be used multiple times
      .persist()

    // Get a map of applicationId => all Attempts made on that ID, sorted by attempt number
    val attempts = logsFormatted
      .filter(line => line.attemptNb > 0)
      // Group lines by application ID and attempt number to enable aggregate on each attempt number
      .groupBy(line => (line.applicationId, line.attemptNb))
      // seqOptAttempts gets all the useful information from all the lines with a given attempt number,
      // then listJoiner simply join every list of attempt returned
      .aggregate[List[Attempt]](Nil)(seqOpAttempts, listJoiner)
      // Sorted by attempt number as requested
      .map(a => ((a.applicationId.toInt, a.attemptNumber), a))
      .toMap

    // RDD of the form (appId -> Array of logs of each containers)
    // Note: key is only None for the end of file (that just contains blank space)
    val aggregatedFailedApps = sc.textFile(aggregatedLogFile).map(x => {
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

    //aggregatedFailedApps.map(x => (x._1, f1(x._2))).collect().foreach(x => print(x))

    // Start by checking all the application Ids from the logs
    // in [startId, endId]
    val allIds = logsFormattedPlain.map(_.split("\n")).flatMap(x => x).map(x => {
      val idPattern = "application_1580812675067_(\\d*)".r

      idPattern.findAllMatchIn(x)
    }).flatMap(x => x).map(x => x.group(1).toInt).filter(x => (x >= startId) && (x <= endId)).collect().toSet

    // Check all application Ids in [startId, endId]
    // that appear in the aggregated logs
    val logsIds =  sc.textFile(aggregatedLogFile).map(appLogContainerPattern.findFirstMatchIn(_))
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

    //val file = "answers.txt"
    //val writer = new BufferedWriter(new FileWriter(file))
    //
    //// First point, show Applications with 121 <= appNb  <= 130
    //apps.filter(app => {
    //  val appNb = Integer.valueOf(app.applicationId)
    //  appNb >= 121 && appNb <= 130
    //})
    //  .collect()
    //  .foreach(a => writer.write(a.toString()))
    //
    //writer.close()
  }

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

    val res = missingClass.orNull

    if (res == null) {
      f2(lines)
    } else {
      res
    }
  }

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

    val res = missingFile.orNull
    if (res == null) {
      f3(lines)
    } else {
      res
    }
  }

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


        if (errorMatch.isDefined)
          ErrorAttempt(3, errorMatch.get.group(2), stage, errorLine)

        else
          null
      } else
        null
    }).headOption

    val res = drvierError.orNull

    if (res == null) {
      f4(lines)
    } else {
      res
    }
  }

  def f4(lines: Iterable[String]): ErrorAttempt = {

    val containerPattern = "Container: container_e02_1580812675067_\\d*_\\d*_(\\d*) on iccluster\\d*\\.iccluster\\.epfl\\.ch.*".r
    val mapContainers = lines.map(x => {
      (containerPattern.findFirstMatchIn(x).get.group(1).toInt, x)
    }).toMap
    // find log of  driver's container
    val driverContainer = mapContainers.get(1).get

    var stageLine = (-1, -1)
    // determine the stage and the source line
    // INFO DAGScheduler: Final stage: ResultStage 0 (collect at App2.scala:22)
    val stageLinePattern = ".*INFO DAGScheduler: Final stage: ResultStage (\\d*) .* at App\\d+.scala:(\\d+) *".r
    val stageLineLine = stageLinePattern.findFirstMatchIn(driverContainer)
    if (stageLineLine.isDefined) {
      val s = stageLineLine.get
      stageLine = (s.group(1).toInt, s.group(2).toInt)
    }

    var tempRes = ErrorAttempt(-1, "", -1, -1)

    //1
    if (driverContainer.contains("than spark.driver.maxResultSize")) {
      tempRes = ErrorAttempt(4, "org.apache.spark.SparkException", stageLine._1, stageLine._2)
    }

    //3 -> driver
    else if (driverContainer.contains("WARN BlockManager: Failed to fetch")) {
      val warnBlockManagerPattern = ".*WARN BlockManager: Failed to fetch .* (.*Exception|.*Error): .*".r
      val excep3 = driverContainer.replace('\n', ' ') match {
        case warnBlockManagerPattern(e) => e.split(":").take(1)(0)
        case _ => ""
      }
      // search exceptions in INFO/ERROR applicationMaster and ERROR Utils
      // otherwise return exception in the line of the corresponding cases
      tempRes = chooseExcep(driverContainer, excep3, stageLine)
    }
    //4 -> driver
    else if (driverContainer.contains("WARN TransportChannelHandler:")) {
      val TransportChannelHandlerPattern = ".*WARN TransportChannelHandler: .* (.*Exception|.*Error): .*".r
      val excep4 = driverContainer.replace('\n', ' ') match {
        case TransportChannelHandlerPattern(e) => e.split(":").take(1)(0)
        case _ => ""
      }
      tempRes = chooseExcep(driverContainer, excep4, stageLine)
    }
    //6 -> driver
    else if (driverContainer.contains("ERROR TaskResultGetter")) {
      val taskResultGetterPattern = ".*ERROR TaskResultGetter: .* (.*Exception|.*Error): .*".r
      val excep6 = driverContainer.replace('\n', ' ') match {
        case taskResultGetterPattern(e) => e.split(":").take(1)(0)
        case _ => ""
      }
      tempRes = chooseExcep(driverContainer, excep6, stageLine)
    }
    //7 -> driver
    else if (driverContainer.contains("ERROR ResourceLeakDetector") || driverContainer.contains("ERROR TransportResponseHandler")) {
      tempRes = chooseExcep(driverContainer, "", stageLine)


    }
    // TBD -> Driver
    else {
      val taskSetManagerPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR TaskSetManager: (.*)\n".r
      val taskSetManagerLine = taskSetManagerPattern.findFirstMatchIn(driverContainer)

      var bool = 0
      if (taskSetManagerLine.isDefined) {
        if (taskSetManagerLine.get.group(1).contains("driver")) {
          tempRes = chooseExcep(driverContainer, "", stageLine)
        }
      }
    }

    val execContainer = mapContainers.filter(x => x._1 > 1)

    if (tempRes.errorCategory == -1) {
      tempRes = execContainer.foldLeft(tempRes)((z, x) => {
        if (z.errorCategory == -1) {
          val line = x._2
          //2 -> executor
          if (line.contains("WARN Executor: Issue communicating with driver in heartbeater") && line.contains("org.apache.spark.rpc.RpcTimeoutException")) {
            chooseExcep(driverContainer, "org.apache.spark.rpc.RpcTimeoutException", stageLine)
          }
          //5 -> executor
          else if (line.contains("ERROR OneForOneBlockFetcher: Failed while starting block fetches")) {
            val blockFetcherPattern = ".*ERROR OneForOneBlockFetcher: Failed while starting block fetches (.*Exception|.*Error): .*".r
            val excep5 = line.replace('\n', ' ') match {
              case blockFetcherPattern(e) => e.split(":").take(1)(0)
              case _ => ""
            }
            chooseExcep(driverContainer, excep5, stageLine)
          }
          // 8
          else if (line.replace('\n', ' ').matches(".*ERROR Executor:.* driver .*")) {
            ErrorAttempt(4, "org.apache.spark.SparkException", stageLine._1, stageLine._2)
          } else {
            z
          }
        } else {
          z
        }
      })
    }


    // if all is not the case
    // call next function
    if (tempRes.errorCategory != -1) {
      tempRes
    } else {
      f5and6(lines)
    }

  }
  // check and choose the exception to return
  def chooseExcep(line: String,excep:String,stageLine:(Int, Int)): ErrorAttempt  ={

    var exception = ""
    val appliMasterPattern = ".*(ERROR|INFO) ApplicationMaster: .* threw exception: (.*Exception):.*".r

    val firstUsefulUtilsMessage = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR Utils: .*\n([^:]*):*".r
    val UtilsExceptionLine = firstUsefulUtilsMessage.findFirstMatchIn(line)
    if (UtilsExceptionLine.isDefined)
      exception = UtilsExceptionLine.get.group(1)


    if (exception.isEmpty) {
      val appliExcep = appliMasterPattern.findFirstMatchIn(line)
      if (appliExcep.isDefined)
        exception = appliExcep.get.group(2)
    }

    if (!exception.isEmpty ){
      ErrorAttempt(4,exception,stageLine._1, stageLine._2)
    } else if (!excep.isEmpty){
      ErrorAttempt(4,excep,stageLine._1, stageLine._2)
    } else{
      null
    }
  }

  def f5and6(lines: Iterable[String]): ErrorAttempt = {
    val ContainerPattern = "Container: container_e02_1580812675067_\\d*_\\d*_(\\d*) on (iccluster\\d*\\.iccluster\\.epfl\\.ch).*".r

    val containers = lines.map(x => {
      val containerLine = ContainerPattern.findFirstMatchIn(x)
      (containerLine.get.group(1).toInt, (containerLine.get.group(2), x))
    }).toMap

    val driverContainer = containers(1)._2

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

    if (exceptionAndType._1 != "" && exceptionAndType._2 != -1 && stage != -1) {
      //println(exceptionAndType._2 + ", "+ exceptionAndType._1 + ", " + stage + ", " + line)
      ErrorAttempt(exceptionAndType._2, exceptionAndType._1, stage, line)
    } else {
      f7(lines, driverContainer)
    }
  }

  def f7(lines: Iterable[String], driverContainerLine: String): ErrorAttempt = {
    val firstErrorPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR".r

    val firstError = firstErrorPattern.findFirstIn(driverContainerLine)
    if (firstError.isDefined) {
      genericFindErrorAttempt(7, driverContainerLine)
    }
    f8(lines)
  }

  def f8(lines: Iterable[String]): ErrorAttempt = {
    val firstErrorPattern = "\\d{2}\\/\\d{2}\\/\\d{2} \\d{2}:\\d{2}:\\d{2} ERROR".r

    val linesWithErrors = lines.filter(line => firstErrorPattern.findFirstIn(line).isDefined)
    if (linesWithErrors.nonEmpty) {
      genericFindErrorAttempt(8, linesWithErrors.head)
    }
    f9(lines)
  }

  def genericFindErrorAttempt(errorCategory: Int, line: String): ErrorAttempt = {
    // Get stage (highest stage information in the log => TODO probably not correct to do that here)
    val stagePattern = "stage (\\d)".r
    val stages = stagePattern.findAllIn(line)
    var stage = -1
    if (!stages.hasNext) {
      stage = stages.map(_.toInt).max
    }

    // Get exception (take the first one available currently)
    val exceptionPattern = "((java\\.[^(\\s|$)]*(Exception|Error)|org\\.apache\\.spark\\.[^(\\s|$)]*(Exception|Error)))".r
    var exception = ""
    val res = exceptionPattern.findFirstIn(line)
    if (res.isDefined) {
      exception = res.get
    }

    // Get line (lowest line information in the log)
    val lineExecutorPattern = "(App\\d$.main\\(App\\d*.scala:(\\d*)\\)|App7.scala:(\\d)\\) failed)".r
    val lines = lineExecutorPattern.findAllIn(line)
    var linNumber = -1
    if (!lines.hasNext) {
      linNumber = stages.map(_.toInt).min
    }

    ErrorAttempt(errorCategory, exception, stage, linNumber)
  }

  def f9(lines: Iterable[String]): ErrorAttempt = {
    // TODO better?
    ErrorAttempt(9, "N/A", -1, -1)
  }

  def listJoiner[U](l1: List[U], l2: List[U]): List[U] = l1 ++ l2

  def seqOpAttempts(list: List[Attempt], attempt: ((String, Int), Iterable[LineData])): List[Attempt] = {
    def folder(agg: Attempt, line: LineData): Attempt = (agg, line) match {
      case (Attempt(a1, a2, _d, endTime, finalStatus, containers), LineData(_, _, _, startTime, true, _, _)) => Attempt(a1, a2, startTime, endTime, finalStatus, containers)
      case (Attempt(a1, a2, startTime, _, _, containers), LineData(_, _, _, endTime, false, finalStatus, null)) => Attempt(a1, a2, startTime, endTime, finalStatus, containers)
      case (Attempt(a1, a2, startTime, endTime, finalStatus, containers), LineData(_, _, _, _, _, _, container)) => Attempt(a1, a2, startTime, endTime, finalStatus, container :: containers)
    }

    val tempNewAttempt: Attempt = attempt._2.foldLeft[Attempt](Attempt(attempt._1._1, attempt._1._2, "", "", "", Nil))(folder)

    val newAttempt = Attempt(tempNewAttempt.applicationId, tempNewAttempt.attemptNumber, tempNewAttempt.startTime, tempNewAttempt.endTime, tempNewAttempt.finalStatus, tempNewAttempt.containers.distinct.sortBy(_._1))

    newAttempt :: list
  }

  def seqOpApps(list: List[SimpleApplication], app: (String, Iterable[LineData])): List[SimpleApplication] = {
    val newApp = app._2.foldLeft[(SimpleApplication, Set[Int])]((SimpleApplication(app._1, 0, ""), Set(0))) {
      case ((SimpleApplication(applicationId, attemptCount, user1), attemptsVals), LineData(_, attemptNb, user2, _, _, _, _)) =>
        val newAttemptCount = if (attemptsVals(attemptNb)) attemptCount else attemptCount + 1
        val newUser = if (user2 == "") user1 else user2
        (SimpleApplication(applicationId, newAttemptCount, newUser), attemptsVals + attemptNb)
    }
    newApp._1 :: list
  }
}
