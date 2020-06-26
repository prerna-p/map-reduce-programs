package graph

import java.io.{BufferedWriter, FileWriter}

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}


object PageRank {

  def main(args: Array[String]) {

    // setup logging
    val log = Logger.getLogger(PageRank.getClass())

    // report error if input or ouput or K are not specified
    if (args.length != 2) {
      log.error("Usage:\ngraph.PageRank <input dir> <output dir>")
      System.exit(1)
    }

    // set spark context
    val conf = new SparkConf().setAppName("Page Rank").setMaster("local")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path("graph"), true) } catch { case _: Throwable => {} }
    //   try { hdfs.delete(new org.apache.hadoop.fs.Path("ranks"), true) } catch { case _: Throwable => {} }
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
    val graph = sc.textFile(args(0)).map(line=>(line.split(",")(0).toInt,line.split(",")(1).toInt))

    var INIT = 1.toFloat / K2.toFloat

    var ranks = sc.textFile(args(0))
        .flatMap(line => line.split(","))
        .map(node=>{if(node.toInt==0)(node.toInt,0.toFloat) else (node.toInt,INIT)})
        .distinct()


    graph.persist()


    for(j<-1 to 1){
      var temp=graph.join(ranks)
      val temp2 = temp.map{case(a,(node,pr))=>(node,pr.asInstanceOf[Float])}.reduceByKey(_+_)
      var delta = temp2.filter{case(x,y)=>x==0}.first()._2
      delta = delta/K2
      var temp3 = temp2.map{case(0,y)=>(0,y) case(x,y)=>(x,y+delta)}
      var temp4 = ranks.leftOuterJoin(temp3).map(row => (row._1,row._2._2))
        .mapValues{case Some(p)=>p case None => delta}
      ranks = temp4
      ranks = ranks.map{case(0,y)=>(0,0) case(x,y)=>(x,y)}
      val count = ranks.map(_._2.asInstanceOf[Float]).sum()
      log.info("**************************************  COUNT : "+count)


    }


    ranks.sortBy(_._1).take(101).foreach(log.info)

    println("***************************************************")
    println(ranks.toDebugString)
    println("***************************************************")

  }


}
