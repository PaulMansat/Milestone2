import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object App8 {
  /**
    * This simple program does the following :
    * 1. Extract (key, value) pairs from a given file,
    * 2. Group those pairs by keys, and sum those values to get a pair (key, sum of values)
    * 3. Extract the maximum "sum-value", and divide it by two (let's call it HALF_MAX)
    * 4. Prints out :
    *   * The maximum value smaller than HALF_MAX
    *   * The minimum value larger than HALF_MAX
    *
    * @param args the file ("small" or "large") to use
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("M2 App8").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("hdfs:///cs449/data2_" +args(0)+ ".csv")
    val rows = lines.map(s => {
      val columns = s.split(",")
      val k = columns(0).toInt
      val v = columns(1).toInt
      (k, v)
    })

    // Solution : Use a custom partitioner instead of the
    // default hash partitioner
    val partitioner = new RangePartitioner(50, rows)
    val partitioned = rows.partitionBy(partitioner)

    val sum = partitioned.groupBy((r: (Int, Int)) => r._1).map{case (k, l) => k -> l.map(_._2).sum}
    val sumvals = sum.map(_._2)
    val mid = sumvals.reduce(Math.max(_, _))/2
    val maxLT = sumvals.filter(_ < mid).reduce(Math.max(_, _))
    val minGT = sumvals.filter(_ > mid).reduce(Math.min(_, _))
    println(s"MAX = $maxLT, Min = $minGT")
  }
}