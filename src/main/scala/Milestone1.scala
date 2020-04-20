import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

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
  }
}

object Milestone1 {

  def main(args: Array[String]): Unit = {
    // Remove unwanted spark logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)

    // Patterns declaration for parsing the data we're interested by or filtering it
    val datePattern = "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})".r
    val userPattern = ".*user: ([a-z]*).*".r
    val launchPattern = "^.*from [A-Z]* to LAUNCHED.*$"
    val finishPattern = ".*State change from [A-Z_]* to (FINISHING|FAILED|KILLED).*".r
    val appRegex = ".*application_1580812675067_\\d*.*|.*appattempt_1580812675067_\\d*.*|.*container_e02_1580812675067_\\d*.*"
    val appNumberPattern = ".*(application_1580812675067_|appattempt_1580812675067_|container_e02_1580812675067_)(\\d*).*".r
    val attemptNbPattern = ".*(appattempt_1580812675067_\\d*_|container_e02_1580812675067_\\d*_)(\\d*).*".r
    val containerPattern = ".*container_e02_1580812675067_\\d*_\\d*_(\\d*).*(iccluster\\d*\\.iccluster\\.epfl\\.ch).*".r

    // Format and extract useful LineData out of the entire log file
    val logsFormatted = sc.textFile(args.head)
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
        case _ => true
      }
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
      .sortBy(a => (a.applicationId, a.attemptNumber))
      // GroupBy application ID to create a map that can be used when creating applications data
      .groupBy(_.applicationId)

    // Now that we have have the formated logs and attemps, we need to use that to create each app information
    val apps = sc.parallelize(
      // Get each of the individual app without the attempts data (SimpleApplication), parallelize the result for better processing later on
      logsFormatted.groupBy(_.applicationId).aggregate[List[SimpleApplication]](Nil)(seqOpApps, listJoiner)
    )
      .sortBy(_.applicationId)
      // Transform the SimpleApplications into Applications that contains all the needed information including Attempts associated to this app
      .map(app => {
        Application(app.applicationId, app.user, app.attemptCount, attempts(app.applicationId))
      })
      // Persist result as it will be reused in each of the questions
      .persist()

    val file = "answers.txt"
    val writer = new BufferedWriter(new FileWriter(file))

    // First point, show Applications with 121 <= appNb  <= 130
    apps.filter(app => {
      val appNb = Integer.valueOf(app.applicationId)
      appNb >= 121 && appNb <= 130
    })
      .collect()
      .foreach(a => writer.write(a.toString()))


    writer.write(questions(apps))
    writer.close()
  }

  /**
    * Answers each of the individual questions (1 to 7)
    * @param apps
    * @return a formatted String of the answers appended
    */
  def questions(apps: RDD[Application]): String = {
    val builder = StringBuilder.newBuilder
    builder.append(question1(apps))
    builder.append(question2(apps))
    builder.append(question3(apps))
    builder.append(question4(apps))
    builder.append(question5(apps))
    builder.append(question6(apps))
    builder.append(question7(apps))
    builder.toString()
  }

  /**
    * Which user has submitted the highest number of applications? How many?
    * USER_NAME, X
    * => for each application we get the user, then group by user and count the total number of times each user appears.
    * All is left is to get the highest (first of sortBy descending values)
    * @param apps
    * @return
    */
  def question1(apps: RDD[Application]): String = {
    val (user, count) = apps.map(app => (app.user, 1)).groupByKey().mapValues(_.toList.length).sortBy(-_._2).first()
    s"1. $user, $count\n\n"
  }

  /**
    * Which user has the highest number of unsuccessful attempts of applications? How many?
    * USER_NAME, X
    * => For each application we get the user of that app, along the count of unsuccessful attempts. Then group by user,
    * total count per user and get the highest (first of sortBy descending values)
    * @param apps
    * @return
    */
  def question2(apps: RDD[Application]): String = {
    val (user, count) = apps.map(app => (app.user, app.attempts.map(_.finalStatus).count(_ != "FINISHING")))
      .groupByKey().mapValues(_.sum).sortBy(-_._2).first()
    s"2. $user, $count\n\n"
  }

  /**
    * List the number of applications that started on the same date for each date on which at least one application started.
    * DATE1: X1, DATE2: X2, ... (date in yyyy-MM-dd format)
    * We define the start of an application by the start of the first of its attempts.
    * => For each application we get the first attempt's date (starting date of this application, keeping only the date information),
    * group by date and count for each date the number of times it appears.
    * @param apps
    * @return
    */
  def question3(apps: RDD[Application]): String = {
    val res = apps.map(_.attempts.head).map(attempt => (attempt.startTime.substring(0, 10), 1))
      .groupByKey().mapValues(_.sum)
      .map(x => s"${x._1}: ${x._2}").collect().mkString(", ")
    s"3. $res\n\n"
  }

  /**
    * What is the mean application duration (from starting the first attempt till the end of the last attempt) in ms (rounded to an integer value)?
    * X
    * => For each application we map it to the result of "subtract the first attempt starting time to the last attempt ending time"
    * and get the mean of the resulting RDD
    * @param apps
    * @return
    */
  def question4(apps: RDD[Application]): String = {
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
    val res = apps.map(app => dateFormat.parse(app.attempts.last.endTime).getTime - dateFormat.parse(app.attempts.head.startTime).getTime)
      .mean().toInt
    s"4. $res\n\n"
  }

  /**
    * What is the mean duration of application attempts that completed successfully in ms (rounded to an integer value)?
    * X
    * => We first get every attempts (using a flatMap), filter for the successfully completed and map them to their duration.
    * And get the mean of the resulting RDD
    * @param apps
    * @return
    */
  def question5(apps: RDD[Application]): String = {
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
    val res = apps.flatMap(_.attempts).filter(_.finalStatus == "FINISHING").map(attempt => dateFormat.parse(attempt.endTime).getTime - dateFormat.parse(attempt.startTime).getTime)
      .mean().toInt
    s"5. $res\n\n"
  }

  /**
    * How many different machines have hosted containers? What are their hostnames, sorted in lexicographic order?
    * X, HOST1, HOST2, ..
    * => We first get every attempts (using a flatMap), then get every containers information (using another flatMap) and map them to their hostname.
    * Then we get each of the distinct hostnames (to have them at most once), all is left is to sort, count and display them in order.
    * @param apps
    * @return
    */
  def question6(apps: RDD[Application]): String = {
    val res = apps.flatMap(_.attempts).flatMap(_.containers).map(_._2).distinct().collect().toList.sorted
    s"6. ${res.length}, ${res.mkString(", ")}\n\n"
  }

  /**
    * Which host launched at least one container from the maximum number of applications? How many?
    * HOSTNAME, X
    * => For each application, we get the list of distinct containers' hostname and convert it to a list of names (flatMap).
    * Then we count how many times each hostname appears in the list, sort them by descending value and return the first one.
    * @param apps
    * @return
    */
  def question7(apps: RDD[Application]): String = {
    val (container, count) = apps
      .flatMap(app => app.attempts.flatMap(_.containers.map(_._2)).distinct)
      .map(container => (container, 1)).groupByKey().mapValues(_.size).sortBy(-_._2).first()
    s"7. $container, $count\n\n"
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