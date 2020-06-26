package graph

import java.io.{BufferedWriter, FileWriter}

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame,Dataset, SparkSession}

object PageRankDS {

  def main(args: Array[String]): Unit = {
    // setup logging
    val log = Logger.getLogger(PageRankDS.getClass())

    // report error if input or ouput or K are not specified
    if (args.length != 2) {
      log.error("Usage:\ntfcount.PageRankDS <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Page Rank DS")
    val sc = new SparkContext(conf)
    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("Spark Reader")
      .getOrCreate
    import spark.implicits._


    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //val hadoopConf = new org.apache.hadoop.conf.Configuration
    //val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //try { hdfs.delete(new org.apache.hadoop.fs.Path("graph"), true) } catch { case _: Throwable => {} }
    //try { hdfs.delete(new org.apache.hadoop.fs.Path("ranks"), true) } catch { case _: Throwable => {} }
    // ================

    // create the graph
    var K = 100//args(2).toInt
    var K2 = K*K
    var s=""
    val fw = new BufferedWriter(new FileWriter(args(0)))
    for(i<-1 to K2){

      if(i%K == 0){
        s = s + i.toString + "," + 0.toString+"\n"
      }
      else {
        s = s + i.toString + "," + (i + 1) + "\n"
      }

    }
    fw.write(s)
    fw.close()


    val dfg = spark.read.text(args(0)).as[String].
      map(line=>(line.split(",")(0).toInt,line.split(",")(1).toInt))

    val graph = dfg.toDF("v1","v2").persist()

    var INIT = 1.0 / K2.toDouble

    var dfr = sc.textFile(args(0))
      .flatMap(line => line.split(","))
      .map(node=>{if(node.toInt==0)(node.toInt,0.0) else (node.toInt,INIT)})
      .distinct()

    var ranks = dfr.toDF("v1","pr")

    for(j<-1 to 10) {
      var temp = graph.join(ranks, "v1")
      temp = temp.drop("v1")
      var temp1 = temp.groupBy("v2").sum("pr")
      var temp2 = ranks.join(temp1, ranks("v1") === temp1("v2"), "leftouter")
      var temp3 = temp2.toDF("v1", "opr", "v2", "pr").drop("opr", "v2").na.fill(0.0)
      var delta = temp3.filter(temp3("v1") === 0).select("pr").first().getDouble(0)
      delta = delta / K2
      var temp4 = temp3.withColumn("pr", temp3("pr") + delta)
      var temp5 = temp4.select(temp4.col("v1"), when(temp4.col("v1") === "0", 0.0)
        .otherwise(temp4.col("pr")) as "pr")
      var sum = temp5.groupBy().sum("pr")
      ranks = temp5
      log.info("*********************************  COUNT : "+ sum.show(1))
    }

    var results = ranks.sort("v1")

    log.info(results.show(101))
  }
}
