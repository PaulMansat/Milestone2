import org.apache.spark.{SparkConf, SparkContext}

object App5 {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("M2 App5").
      set("spark.yarn.maxAppAttempts", "1").
      set("spark.yarn.max.executor.failures", "1").
      set("spark.task.maxFailures", "1")

    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("hdfs:///cs449/data5_" + args(0) +".csv")
    val rows = lines.map(s => {
      val columns = s.split(",")
      val k = columns(0).toInt
      val v = columns(1).toInt
      (k, v)
    })

    /*
    Original code:
      val extendedRows = rows.mapPartitions(it  => {
      val l = it.toList
      val l2 = l.flatMap(x => (2 to 50).map(i => (x._1, x._2 % i)))
      l2.iterator
      })
      val sum = extendedRows.groupBy(_._1).map { case (k, l) => k -> l.map(_._2).sum}
    We now update the code according to the solution described in the report (i.e reduce the size of the intermediate
    data-set extendedSum by doing operation on-the-fly). To do so, we first the data structure by key and apply a
    map-foldleft to do the sum of the modulo of each elements
     */

    val sum = rows.groupBy(_._1).map{case (k, l) => k -> l.map{case (_, n) => (2 to 50).foldLeft(0){case (acc, i) => acc + (n%i)}}.sum}
    //val sum = rows.groupBy(_._1).map{case (z) => z._1 -> z._2.foldLeft(0){case(acc, n) => acc + (2 to 50).foldLeft(0){case (acc2, i) => acc2 + (n._2 % i)}}}
    sum.collect().foreach{case (x1, x2) => println("1: " + x1 + " 2: " + x2)}

  }
}