package autoreLibro

import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("hello, world2!")
    val conf = new SparkConf().setMaster("local[2]").setAppName("CountingSheep")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    distData.mapPartitionsWithIndex((index: Int, it: Iterator[Int]) => it.toList.map(x => if (index == 0) {
      println(x + " " + index)
    }).iterator).collect
    //rdd da 1 a 100
    println("rdd da 1 a 100")
    val array100 = 1 to 100
    val rdd100 = sc.parallelize(array100)
    rdd100.foreach(x => print(x.toString))
    //rdd al quadrato
    println("rdd da 1 a 100 al quadrato")
    val rddQuadrato = rdd100.map(x => x * x)
    rddQuadrato.foreach(println)
    //rdd non divisibili per 3 filter
    println("rdd da 1 a 100 al quadrato filtrato solo multipli di 3")
    val rddNonDivisibilePerTre = rddQuadrato.filter(x => x % 3 == 0)
    rddNonDivisibilePerTre.foreach(println)
    //rdd da 1 a 5 fatmap
    println("flat map di un rdd da 1 a 5 dove per ogni valore si crea un array di dimenzioni del valore")
    val arrayDa1a5 = Array(1, 2, 3, 4, 5)
    val rddDa1a5 = sc.parallelize(arrayDa1a5)
    val rddFlatmap = rddDa1a5.flatMap(x => 1 to x)
    rddFlatmap.foreach(println)
    //somma elementi rdd
    println(rdd100.sum)
  }
}
