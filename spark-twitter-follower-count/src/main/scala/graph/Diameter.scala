package graph

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object Diameter {

  def main(args: Array[String]) {
    val log = Logger.getLogger(Diameter.getClass())


    if (args.length != 2) {
      log.error("Usage:\ngraph.Diameter <input dir> <output dir>")
      System.exit(1)
    }

    // set spark context
    val conf = new SparkConf().setAppName("Diameter RDD")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //try { hdfs.delete(new org.apache.hadoop.fs.Path("graph1"), true) } catch { case _: Throwable => {} }
    //try { hdfs.delete(new org.apache.hadoop.fs.Path("output"), true) } catch { case _: Throwable => {} }

    val csvFile = sc.textFile(args(0))
    var edges = csvFile.map(line => (line.split(",")(0),line.split(",")(1)))
        .groupBy(x=>x._1)
        .mapValues(_.map(_._2).mkString(","))

    var INF = Int.MaxValue.toString;
    var SRC = "1"

    var distances = edges.map{case (x,y) => if (x.equals(SRC)) (x,(y,"0")) else (x,(y,INF))}

    distances.collect().foreach(println)
    for(i<-0 to 10) {
      edges.persist()
      var temp = edges.join(distances)

      var temp1 = temp.mapValues { case (x, y) => y }

      var temp2 = temp1.map { case (x, y) => y }

      var temp3 = temp2.map { case (x, y) => extractVertices(x, y) }


      var temp4 = temp3.map({ case x => if (x.contains('|')) (x.split('|')(0), x.split('|')(1)) else (x.split(",")(0), x.split(",")(1)) })
      var temp5 = temp4.map({ case (x, y) => if (x.split(",").size > 1) (x + '_' + y) else (x + ',' + y) })
        .flatMap { case x => if (x.contains('_')) x.split('_') else x.split("\n") }
        .map(x => (x.split(",")(0), x.split(",")(1))).reduceByKey((x, y) => ((Integer.parseInt(x) + Integer.parseInt(y)).toString))
      temp5.collect().foreach(log.info)
    }


  }

  def extractVertices(str: String, dMax: String): String ={

    if(!str.contains(",")){

      if(dMax.equals(Int.MaxValue.toString))
        return str+","+dMax
      else{
        var temp = dMax.toInt + 1
        return str+","+temp
      }

    }

    var x = str.split(",")
    var res=""
    var dist = dMax.toInt
    var in_dist = dist + 1

    for(i<-0 to x.size-1){
      res = res + x(i) + "," + in_dist + "|"
    }
    return res

  }


}