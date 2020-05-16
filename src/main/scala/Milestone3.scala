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

object Milestone1 {

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
    val datePattern = "(\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2})".r
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

    aggregatedFailedApps.map(x => (x._1, f1(x._2)))

    // Special case of error type 1 : A missing jar does not create
    // containers (and thus containers logs)
    // Of the form (appId, ErrorAttempt)
    // /!\ awful
    val missingJarCategory1 = logsFormattedPlain.map(x => {
      val error = ".* INFO Client: Deleted staging directory .*iccluster\\d*\\.iccluster\\.epfl\\.ch" +
        ".*application_1580812675067_(\\d*)\nException in thread \"main\" java.io.FileNotFoundException: File .* does not exist"
      val errorPattern = error.r

      errorPattern.findAllMatchIn(x)
    }).flatMap(x => x).map(x => x.group(1).toInt).filter(x => (x >= startId) && (x <= endId)).map(x => (x, ErrorAttempt(1, "java.io.FileNotFoundException", -1, -1)))

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
    // find driver's container, if not, return null
    //val containerPattern = "Container: container_e02_1580812675067_\\d*_\\d*_000001 on iccluster\\d*\\.iccluster\\.epfl\\.ch.*".r
    //val driver_container = lines.filter(_.matches("^Container: container_.*0001 .*$")).isEmpty
    //if (driver_container){
    //  return null
    //}
    val containerPattern = "Container: container_e02_1580812675067_\\d*_\\d*_000001 on iccluster\\d*\\.iccluster\\.epfl\\.ch.*".r

    val type4res= lines.filter(line => containerPattern.findFirstMatchIn(line).isDefined).map(line => {

      // determine the stage and the source line
      // INFO DAGScheduler: Final stage: ResultStage 0 (collect at App2.scala:22)
      val stageLinePattern = ".*INFO DAGScheduler: Final stage: ResultStage (\\d*) .* at App\\d+.scala:(\\d+) *".r
      val stageLine = line match {
        case stageLinePattern(st, l) => (st.toInt, l.toInt)
        case _ => (-1, -1)
      }

      // search exceptions in applicationMaster and utils
      // otherwise return exception in the line of WARN Executor
      val appli_utilExcep = f4_findErrorInAppMasterAndErrorUtil(lines)

      // 1
      if (line.contains("WARN Executor: Issue communicating with driver in heartbeater") && line.contains("org.apache.spark.rpc.RpcTimeoutException")) {
        val e1 = chooseExcep(lines, "org.apache.spark.rpc.RpcTimeoutException", appli_utilExcep, stageLine)
        e1
      }
      //2
      else if (line.contains("WARN BlockManager: Failed to fetch")) {
        //WARN BlockManager: Failed to fetch ... (*Exception): Exception thrown in awaitResult:
        val warnBlockManagerPattern = ".*WARN BlockManager: Failed to fetch .* (.*Exception|.*Error): .*".r
        val excep2 = line.replace('\n', ' ') match {
          case warnBlockManagerPattern(e) => e
          case _ => ""
        }

        chooseExcep(lines, excep2, appli_utilExcep, stageLine)

      }
      //3 
      else if (line.contains("spark.driver.maxResultSize")) {
        val maxResultPattern = ".*ApplicationMaster: .* (.*Exception|.*Error): Job aborted due to stage failure: Total size of serialized results of .* than spark.driver.maxResultSize .*".r
        val excep3 = line.replace('\n', ' ') match {
          case maxResultPattern(e) => e
          case _ => ""
        }
        chooseExcep(lines, excep3, appli_utilExcep, stageLine)
      }
      //4
      else if (line.contains("WARN TransportChannelHandler:")) {
        val TransportChannelHandlerPattern = ".*WARN TransportChannelHandler: .* (.*Exception|.*Error): .*".r
        val excep4 = line.replace('\n', ' ') match {
          case TransportChannelHandlerPattern(e) => e
          case _ => ""
        }
        chooseExcep(lines, excep4, appli_utilExcep, stageLine)
      }
      //5
      else if (line.contains("ERROR OneForOneBlockFetcher: Failed while starting block fetches")) {
        val blockFetcherPattern = ".*ERROR OneForOneBlockFetcher: Failed while starting block fetches (.*Exception|.*Error): .*".r
        val excep5 = line.replace('\n', ' ') match {
          case blockFetcherPattern(e) => e
          case _ => ""
        }
        chooseExcep(lines, excep5, appli_utilExcep, stageLine)
      }
      //6
      else if (line.contains("ERROR Executor:")) {
        val excutorErrorWithDriverPattern = ".*ERROR Executor: .* driver .*"
        val excep5 = line.replace('\n', ' ').matches(excutorErrorWithDriverPattern)
        ErrorAttempt(4, "org.apache.spark.SparkException", stageLine._1, stageLine._2)
      }
      //7
      else if(line.contains("ERROR ResourceLeakDetector") || line.contains("ERROR TaskResultGetter") || line.contains("ERROR TransportResponseHandler")){
        chooseExcep(lines, "", appli_utilExcep, stageLine)
      }
      else{
        null
      }

    // end map
    }).headOption

    // if all is not the case
    // call next function
    val res = type4res.orNull
    if (res == null) {
      f5(lines)
    } else {
      res
    }
  }

  // check and choose the exception to return
  def chooseExcep(lines: Iterable[String],excep:String,appli_utilExcep:(String,String),stageLine:(Int, Int)): ErrorAttempt  ={
    if ( !appli_utilExcep._1.isEmpty ){
      ErrorAttempt(4,appli_utilExcep._1,stageLine._1, stageLine._2)
    } else if(!appli_utilExcep._2.isEmpty){
      ErrorAttempt(4,appli_utilExcep._2,stageLine._1, stageLine._2)
    } else if (!excep.isEmpty){
      ErrorAttempt(4,excep,stageLine._1, stageLine._2)
    } else{
      null
    }
  }

  def f4_findErrorInAppMasterAndErrorUtil(lines: Iterable[String]):(String,String) = {
    val appliMasterPattern = ".*(ERROR|INFO) ApplicationMaster: .* threw exception: (.*Exception:) .*".r
    val appliExcep = lines.filter(l=> l.contains("ApplicationMaster:")).map(_.replace('\n',' ')).map {
      _ match {
        case appliMasterPattern (_, e) => e.split (":").take(1)(0)
        case _ => ""
      }
    }

    val errUtilsPattern = ".*ERROR Utils: .* task-result-getter-\\d+(.*) .*".r
    val utilExcep = lines.filter(l=> l.contains("ERROR Utils:")).map(_.replace('\n',' ')).map {
      _ match {
        case errUtilsPattern(e) => e.split (":").take(1)(0)
        case _ => ""
      }
    }
    (appliExcep.toString(),utilExcep.toString())
  }

  def f5(lines: Iterable[String]): ErrorAttempt = {
    val res = ???
    if (res == null) {
      f6(lines)
    } else {
      res
    }
  }

  def f6(lines: Iterable[String]): ErrorAttempt = {
    val res = ???
    if (res == null) {
      f7(lines)
    } else {
      res
    }
  }

  def f7(lines: Iterable[String]): ErrorAttempt = {
    val res = ???
    if (res == null) {
      f8(lines)
    } else {
      res
    }
  }

  def f8(lines: Iterable[String]): ErrorAttempt = {
    val res = ???
    if (res == null) {
      f9(lines)
    } else {
      res
    }
  }

  def f9(lines: Iterable[String]): ErrorAttempt = {
    ???
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
