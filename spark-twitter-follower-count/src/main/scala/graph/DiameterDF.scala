package graph


import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame,Dataset, SparkSession}

object DiameterDF {
  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(DiameterDF.getClass())

    // report error if input or ouput or K are not specified
    if (args.length != 2) {
      log.error("Usage:\ntfcount.Diameter <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Diameter DS")
    val sc = new SparkContext(conf)
    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("Spark Reader")
      .getOrCreate
    import spark.implicits._


    var dfg = spark.read.text(args(0)).as[String].map(line=>(line.split(",")(0).toInt,line.split(",")(1).toInt))
    var graph = dfg.toDF("id1","id2","dis")

    var distance = graph.select(graph.col(“id1”,”w”))

    for (i<-1 to 10){
      graph.persist()
      var temp = distance.select(distance.col("id1"),
        when(distance.col("id") == graph.col(“id1”)
      .withColumn(distance.col(“w”), graph.col(“dis”)+ distance.col(“w”))
      var temp1 = distances.join(temp)
      distance= temp1.select(col(“id”,”w”))
    }

  }



}
