package Progetto

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime



object ParserJson {
  val NUMERO_ROWS=15
  val configuretion= new SparkConf().setAppName("Spark json extract")

  configuretion.setMaster("local")


  val sc = new SparkContext(configuretion)
  val sqlContext= new SQLContext(sc)
  val hiveContext=new HiveContext(sc)

  import hiveContext.implicits._
  def main(args: Array[String]): Unit = {

    val df = sqlContext.read.json("C:\\Users\\Corsista\\Desktop\\BigData\\progetto\\file.json")
    df.registerTempTable("dati")

    val new_df= df.withColumnRenamed("public","publicFild")
    val rdd=new_df.as[Event].rdd
/*

    //TODO: 1.1) Trovare i singoli «actor»;
    //DataFrame
    val dfActor = new_df.select("actor").distinct()
    dfActor.show()
    //RDD
    val rddActor = rdd.map(x => x.actor).distinct()
    rddActor.take(NUMERO_ROWS).foreach(println)
*/

/*

    //TODO:1.2)Trovare i singoli «author», presente all’interno dei commit;
    //_____________________________creo DataFrame di commits_____________________________________
    val dfPayload = df.select("payload.*")
    val dfCommit = dfPayload.select(explode(col("commits"))).select("col.*")
    //___________________________________________________________________________________________
    //DataFrame
    val dfAuthorCommit = dfCommit.select("author").distinct()
    dfAuthorCommit.show()
    //RDD
    val rddCommit = dfCommit.as[Commit].rdd
    val rddAuthor = rddCommit.map(x => x.author).distinct()
    rddAuthor.take(NUMERO_ROWS).foreach(println)
*/


/*
    //TODO:1.3)Trovare i singoli «repo»;
    //DataFrame
    val dfRepo = new_df.select("repo").distinct()
    dfRepo.show()
    //RDD
    val rddRepo = rdd.map(x => x.repo).distinct()
    rddRepo.take(NUMERO_ROWS).foreach(println)
*/

/*

    //TODO:1.4)Trovare i vari tipi di evento «type»;
    //DataFrame
    val dfType = new_df.select("`type`").distinct()
    dfType.show()
    //RDD
    val rddType = rdd.map(x => x.`type`).distinct()
    rddType.take(NUMERO_ROWS).foreach(println)
*/

/*

    //TODO:1.5)Contare il numero di «actor»;
    //DataFrame
    val dfNumeroActor= new_df.select("actor").distinct().count()
    println(dfNumeroActor)
    //RDD
    val rddNumeroActor = rdd.map(x => x.actor).distinct().count()
    println(rddNumeroActor)
*/

/*

    //TODO:1.6)Contare il numero di «repo»;
    //DataFrame
    val dfNumeroRepo = new_df.select("repo").distinct().count()
    println(dfNumeroRepo)
    //RDD
    val rddNumeroRepo = rdd.map(x => x.repo).distinct().count()
    println(rddNumeroRepo)
*/


   /* //TODO:2.1)Contare il numero di «event» per ogni «actor»;
    //DataFrame
    val dfNumeroEventForActor = new_df.groupBy( "actor").count().show()
    println(dfNumeroEventForActor)
    //RDD
    val rddNumeroEventForActor = rdd.map(x => (x.actor, 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)
    rddNumeroEventForActor.take(NUMERO_ROWS).foreach(println)
*/

   /* //TODO: 2.2)Contare il numero di «event», divisi per «type» e «actor»;
    //DataFrame
    val dfNumeroEventDivideTypeAndActor = new_df.select(($"type"), ($"actor"), count($"*").over(Window.partitionBy("type", "actor")) as "nEvent")
    dfNumeroEventDivideTypeAndActor.show()
    //RDD
    val rddNumeroEventDivideTypeAndActor = rdd.map(x => ((x.`type`, x.actor), 1L)).reduceByKey((e1,e2) => e1+e2)
    rddNumeroEventDivideTypeAndActor.take(NUMERO_ROWS).foreach(println)*/
/*

    //TODO: 2.3)Contare il numero di «event», divisi per «type», «actor», «repo»;
    //DataFrame
    val dfNumeroEventDivideTypeActorAndRepo = new_df.select($"type", $"actor", $"repo", count($"*").over(Window.partitionBy($"type", $"actor", $"repo")) as "nEvent")
    dfNumeroEventDivideTypeActorAndRepo.show()
    //RDD
    val rddNumeroEventDivideTypeActorAndRepo = rdd.map(x => ((x.`type`, x.actor, x.repo), 1L)).reduceByKey((e1,e2) => e1+e2)
    rddNumeroEventDivideTypeActorAndRepo.take(NUMERO_ROWS).foreach(println)
*/


   /* //TODO: 2.4)Contare il numero di «event», divisi per «type», «actor», «repo» e ora;
    //DataFrame
    val dfTARO = new_df.withColumn("second", second($"created_at"))
    val dfContTARO = dfTARO.select($"type", $"actor", $"repo", $"second", count($"*").over(Window.partitionBy($"type", $"actor", $"repo", $"second")) as "nEvent")
    dfContTARO.show()
    //RDD
    val rddContTARO = rdd.map(x=> ((x.`type`, x.actor, x.repo, new DateTime(x.created_at.getTime).getSecondOfMinute), 1L)).reduceByKey((contatore1, contatore2) => contatore1 + contatore2)
    rddContTARO.take(NUMERO_ROWS).foreach(println)
    */

/*
    //TODO: 2.5)Trovare il massimo/minimo numero di «event» per ora;
    //DataFrame
    val dfOra = new_df.withColumn("second", second($"created_at"))
    val dfEventOra = dfOra.select($"second", count($"*").over(Window.partitionBy($"second")) as "count")
    val dfEventMinOra = dfEventOra.agg(max("count"))
    val dfEventMaxOra = dfEventOra.agg(min("count"))
    dfEventMaxOra.show()
    dfEventMinOra.show()
    //RDD
    val rddEventOra = rdd.map(x=>(x.created_at.getTime, 1L)).reduceByKey((count1, count2)=> count1 + count2)
    val rddEventMaxOra = rddEventOra.map(x => x._2).max()
    val rddEventMinOra = rddEventOra.map(x => x._2).min()
    println("Max conteggio RDD " + dfEventMaxOra)
    println("Min conteggio RDD " + dfEventMinOra)
*/

 /*   //TODO: 2.6)Trovare il massimo/minimo numero di «event» per «actor»;
    //DataFrame
    val count_df = new_df.select(col("actor"), count($"*").over(Window.partitionBy("actor")) as "newCount")
    val max_event_df= count_df.agg(max("newCount"))
    val min_event_df= count_df.agg(min("newCount"))
    max_event_df.show()
    min_event_df.show()
    //RDD
    val count_RDD = rdd.map(x=>(x.actor, x)).aggregateByKey(0)((count, actor)=> count + 1, (count1, count2)=> count1 + count2 )
    val max_count_RDD = count_RDD.map(x=>x._2).max()
    val min_count_RDD = count_RDD.map(x=>x._2).min()
    println("Max conteggio RDD " + max_count_RDD)
    println("Min conteggio RDD " + min_count_RDD)
*/
    /*
    //TODO: 2.7)Trovare il massimo/minimo numero di «event» per «repo»;
    //DataFrame
    val count_repo_df = new_df.select(col("repo"), count($"*").over(Window.partitionBy("repo")) as "count")
    val max_repo_df = count_repo_df.agg(max("count"))
    val min_repo_df = count_repo_df.agg(min("count"))
    max_repo_df.show()
    min_repo_df.show()
    //RDD
    val count_repo_RDD = rdd.map(x=>(x.repo, x)).aggregateByKey(0)((count, repo)=> count + 1, (count1, count2)=> count1 + count2 )
    val max_repo_RDD = count_repo_RDD.map(x=>x._2).max()
    val min_repo_RDD = count_repo_RDD.map(x=>x._2).min()
    println("Max conteggio RDD " + max_repo_RDD)
    println("Min conteggio RDD " + min_repo_RDD)
*/
 /*
    //TODO: 2.8)Trovare il massimo/minimo numero di «event» per ora ora per «actor»;
    //DataFrame
    val second_actor_df = new_df.withColumn("second", second($"created_at"))
    val count_esa_df = second_actor_df.select(col("actor"), col("second"), count($"*").over(Window.partitionBy("actor", "second")) as "count")
    val max_event_second_actor = count_esa_df.agg(max("count"))
    val min_event_second_actor = count_esa_df.agg(min("count"))
    max_event_second_actor.show()
    min_event_second_actor.show()
    //RDD
    val count_esa_RDD = rdd.map(x=>((x.actor,x.created_at.getTime), 1L)).reduceByKey((count1, count2) => count1 + count2)
    val max_esa_RDD = count_esa_RDD.map(x=>x._2).max()
    val min_esa_RDD = count_esa_RDD.map(x=>x._2).min()
    println("Max conteggio RDD " + max_esa_RDD)
    println("Min conteggio RDD " + min_esa_RDD)
*/

 /*   //TODO: 2.9)Trovare il massimo/minimo numero di «event» per ora per «repo»;
    //DataFrame
    val second_repo_df = new_df.withColumn("second", second($"created_at"))
    val count_esr_df = second_repo_df.select(col("repo"), col("second"), count($"*").over(Window.partitionBy("repo", "second")) as "count")
    val max_event_second_repo = count_esr_df.agg(max("count"))
    val min_event_second_repo = count_esr_df.agg(min("count"))
    max_event_second_repo.show()
    min_event_second_repo.show()
    //RDD
    val count_esr_RDD = rdd.map(x=>((x.repo,x.created_at.getTime), 1L)).reduceByKey((count1, count2) => count1 + count2)
    val max_esr_RDD = count_esr_RDD.map(x=>x._2).max()
    val min_esr_RDD = count_esr_RDD.map(x=>x._2).min()
    println("Max conteggio RDD " + max_esr_RDD)
    println("Min conteggio RDD " + min_esr_RDD)
  */
    /*
     //TODO: 2.10)trova max e min numero di event per secondo per repo e actor
    //DF_max
    val df_mx_raS = df.withColumn( "second", second($"created_at"))
    val data_frame_max_repoactorS = df_mx_raS.select( $"second", $"repo", $"actor", count($"*").over(Window.partitionBy( $"second", $"repo", $"actor")) as "conteggio")
    val df_max_repoactorS = data_frame_max_repoactorS.agg(max("conteggio"))
    df_max_repoactorS.show()
    //RDD_max
    val rdd_max_repoactorS = rdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo, x.actor.id), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_max_raS = rdd_max_repoactorS.map(x => x._2).max()
    println(rdd_max_raS)
    //DF_min
    val df_mn_raS = df.withColumn( "second", second($"created_at"))
    val data_frame_min_repoactorS = df_mn_raS.select( $"second", $"repo", $"actor", count($"*").over(Window.partitionBy( $"second", $"repo", $"actor")) as "conteggio")
    val df_min_repoactorS = data_frame_min_repoactorS.agg(min("conteggio"))
    df_min_repoactorS.show()
    //RDD_min
    val rdd_min_repoactorS = rdd.map(x => ((new DateTime(x.created_at.getTime).getSecondOfMinute, x.repo, x.actor.id), x)).aggregateByKey(0)((contatore, actor) => contatore + 1, (contatore1, contatore2) => contatore1 + contatore2)
    val rdd_min_raS = rdd_min_repoactorS.map(x => x._2).min()
    println(rdd_min_raS)
    */
    /*
    //TODO: 3.1)contare il numero di commit
    val dfPayload = df.select("payload.*")
    val dfCommit = dfPayload.select(explode(col("commits"))).select("col.*")
    //DF
    val df_nComit = dfCommit.distinct().count()
    println(df_nComit)
    //RDD
    val rdd_nCommit = dfCommit.distinct().count()
    println(rdd_nCommit)
*/
  }

}
