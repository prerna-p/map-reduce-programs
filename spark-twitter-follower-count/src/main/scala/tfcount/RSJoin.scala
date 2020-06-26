package tfcount

/*
 * this program calculates the number of followers for a user, given the
 * input in the form of lines consisting of (user, user-it-follows)
 */

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object RSJoin {

  def main(args: Array[String]) {

    // setup logging
    val log = Logger.getLogger(RSJoin.getClass())

    // report error if input or ouput are not specified
    if (args.length != 2) {
      log.error("Usage:\ntfcount.RSJoin <input dir> <output dir>")
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
    val csvFile = sc.textFile(args(0))

    val MAX = 21000;
    val filterRDD = csvFile.filter(line => line.split(",")(0).toInt < MAX &&
      line.split(",")(1).toInt < MAX)
    val from = filterRDD.map(line => (line.split(",")(0),line.split(",")(1)))
    val to =  filterRDD.map(line => (line.split(",")(1),line.split(",")(0)))
    val joinRDD = from.join(to)

    val flipRDD = joinRDD.map(
      {case (start,(mid,end)) =>((mid,end),start)})

    val cFrom = from.map(
      {case (to,fr) => ((to,fr),null)})

    val result = flipRDD.join(cFrom).count()
    log.info("\n****************************************************************")
    log.info("\n"+result/3)
    log.info("\n****************************************************************")

  }
}
