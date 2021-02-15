package autoreLibro

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object EsercizioDataFrame {

  def main(args: Array[String]): Unit = {
    val autore1 = Autore(1, "mario", "rossi", 36)
    val autore2 = Autore(2, "marco", "verdi", 42)

    val libro1 = Libro(1, "libro1", 234, "1960", 1)
    val libro2 = Libro(2, "libro2", 544, "1967", 1)
    val libro3 = Libro(3, "libro3", 164, "1979", 1)
    val libro4 = Libro(4, "libro4", 756, "1967", 2)
    val libro5 = Libro(5, "libro5", 121, "1977", 2)


    val autori = Array(autore1, autore2)
    val libri = Array(libro1, libro2, libro3, libro4, libro5, libro3)

    val configurazioneSpark = new SparkConf().setMaster("local[2]").setAppName("CountingSheep")
    val contextSpark = new SparkContext(configurazioneSpark)
    contextSpark.setCheckpointDir("C:\\Users\\Corsista\\Desktop\\BigData\\checkpoint")
    val sqlContext = new SQLContext(contextSpark)
    val jsonDF = sqlContext.read.json("C:\\Users\\Corsista\\Desktop\\BigData")


    val rddAutori = contextSpark.parallelize(autori)
    val rddLibri = contextSpark.parallelize(libri)

  }
}
