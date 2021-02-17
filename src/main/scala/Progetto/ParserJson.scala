package Progetto

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext


object ParserJson {
  val configuretion= new SparkConf().setAppName("Spark json extract")

  configuretion.setMaster("local")


  val sc = new SparkContext(configuretion)
  val sqlContext= new SQLContext(sc)
  val hiveContext=new HiveContext(sc)

  import hiveContext.implicits._
  def main(args: Array[String]): Unit = {

    val df = sqlContext.read.json("C:\\Users\\Corsista\\Desktop\\BigData\\progetto\\file.json")
    df.registerTempTable("dati")
    val data= sqlContext.sql("select * from dati")
    data.show()

    val dataActor= sqlContext.sql("select actor from dati")
    dataActor.show()

    val dataRepo= sqlContext.sql("select repo from dati")
    dataRepo.show()

    val dataCont= sqlContext.sql("select cont(actor) from dati")
    dataCont.show()
    sc.stop


  }

}
