import org.apache.spark.{SparkConf, SparkContext}

object App3 {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("M2 App3").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("hdfs:///cs449/data1_" + args(0) +".csv")
    val rows = lines.map(s => {
      val columns = s.split(",")
      val k = columns(0).toInt
      val v = columns(1).toInt

      (k, v)
    })

    /*
    Original code:
      val sum = rows.groupBy(_._1).map { case (k, l) => k -> l.map(_._2).sum }.collect
      val extendedSum = sum.flatMap(x => (2 to 100).map(i => x._1 -> x._2 % i))
      val res = extendedSum.foldLeft(0){case (acc, cur) => acc + cur._1 * cur._2}
    We now update the code according to the solution described in the report (i.e reduce the size of the intermediate
    data-set extendedSum by doing operation on-the-fly).
     */
    val sum = rows.groupBy(_._1).map { case (k, l) => k -> l.map(_._2).sum }
    val extendedSum = sum.map{ case (x) => x._1 * (2 to 100).foldLeft(0){case (acc, i) => acc + x._2 % i}}

    val res = extendedSum.collect.sum
    println("Result = " + res)
  }
}