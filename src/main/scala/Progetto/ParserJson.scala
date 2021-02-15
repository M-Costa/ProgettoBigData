package Progetto

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object ParserJson {
  val configuretion= new SparkConf().setAppName("Spark json extract")

  configuretion.setMaster("local")


  val sc = new SparkContext(configuretion)
  val sqlContext= new SQLContext(sc)

  def main(args: Array[String]): Unit = {

    val df = sqlContext.read.json("C:\\Users\\Corsista\\Desktop\\BigData\\progetto\\file.json")
    df.registerTempTable("dati")
    val data= sqlContext.sql("select * from dati")
    data.show()
    sc.stop


  }

}
