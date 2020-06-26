package tfcount

/*
 * this program calculates the number of followers for a user, given the
 * input in the form of lines consisting of (user, user-it-follows)
 */

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object RDDA {

  def main(args: Array[String]) {

    // setup logging
    val log = Logger.getLogger(RDDA.getClass())

    // report error if input or ouput are not specified
    if (args.length != 2) {
      log.error("Usage:\ntfcount.RDDA <input dir> <output dir>")
      System.exit(1)
    }

    // set spark context
    val conf = new SparkConf().setAppName("Twitter Followers Count")
    val sc = new SparkContext(conf)
    log.error(sc)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    // works only for edges.csv
    val csvFile = sc.textFile(args(0))

    /* count followers
     * for each pair of input line, split by comma and discard the first field
     * sum up follow counts per user-id
     */
    val counts = csvFile.map(line => (line.split(",")(1),1))
      .aggregateByKey(0)(_+_,_+_)
    // display the execution report
    log.info("\n****************************************************************")
    log.info("\n"+counts.toDebugString)
    log.info("\n****************************************************************")
    counts.saveAsTextFile(args(1))
  }
}
