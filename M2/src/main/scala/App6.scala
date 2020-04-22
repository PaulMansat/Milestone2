import org.apache.spark.{SparkConf, SparkContext}


object App6 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("M2 App6").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)
    
    val file = "hdfs:///cs449/data3_"+args(0)+".csv"
    val data = sc.textFile(file)
    val rows = data.map(line => {
      val c = line.split(",")
      (c(0), c(1).toInt, c(2).toInt)
    })
    
    // We want to group by (c0, c1) and sum c2 in each group.
    // It was first grouping by c0 and then by c1, having the second operation done locally on the nodes (not as a RDD operation anymore).
    // The reste was about formating it correctly. Instead we now directly group by (c0, c1) and sum c2 without needing any additional
    // operation as the formatting is already correct.
    val t1 = rows.map(row => (row._1, row._2) -> row._3).groupByKey()
    val t2 = t1.mapValues(_.sum)
    t2.collect.foreach(println)

  }
}
