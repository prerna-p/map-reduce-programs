package tfcount

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object DsRepJoin {
  def main(args: Array[String]) {

    // setup logging
    val log = Logger.getLogger(DsRepJoin.getClass())

    // report error if input or ouput are not specified
    if (args.length != 2) {
      log.error("Usage:\ntfcount.DsRSJoin <input dir> <output dir>")
      System.exit(1)
    }

    // set spark context
    val conf = new SparkConf().setAppName("Twitter Followers Count")
    val sc = new SparkContext(conf)
    //log.error(sc)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    // works only for edges.csv

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("Spark CSV Reader")
      .getOrCreate

    import spark.implicits._

    val ds = spark.read
      .text(args(0)).as[String]


    val MAX = 25000;
    val filterDS = ds.filter(line => line.split(",")(0).toInt < MAX && line.split(",")(1).toInt < MAX)
    val from = filterDS.map(line => (line.split(",")(0),line.split(",")(1)))
    val to =  filterDS.map(line => (line.split(",")(1),line.split(",")(0)))
    val fromDf = from.toDF("FROM","TO")
    val tDf = to.toDF("FROM","TO")

    val broadcast1 = spark.sparkContext.broadcast(tDf.collectAsList())
    val bs = spark.sparkContext.broadcast(tDf.schema)
    val bsDf = spark.createDataFrame(broadcast1.value,bs.value)
    // find 2 length paths
    val joinTemp = fromDf.join(bsDf,"FROM")

    val joinDf = joinTemp.toDF("MID","END","START")
    val flipTo = fromDf.toDF("END","START")

    val result = flipTo.join(joinDf,Seq("END","START")).count()
    log.info("\n****************************************************************")
    log.info("\n"+result/3)
    log.info("\n****************************************************************")
  }
}
