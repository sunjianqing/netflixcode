package com.jianqing.scala.netflix

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianqingsun on 11/7/17.
  *
  * 比如有一张表10个fields， 加一个userid
  * 我们知道10个fields 的所有可能取值， 这个时候， 就可以build 一个broadcast 的表， 然后用data source 表 去"join" 这个表，如果join
  * 上了，就可以输出记录， 没join 上， 就忽略了， 然后通过groupbykey， 或者reducebykey 就可以统计数量了
  */
object BroadCastDemo extends App{


  override def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("NetworkWordCount")
    val sc = new SparkContext(conf)


    val list = Seq("CHN_iphone", "CHN_mac", "CHN_ipad", "USA_mac", "USA_iphone", "USA_ipad", "CAN_mac", "CAN_iphone", "CAN_ipad")
    val allPossiblities : Broadcast[Seq[String]] = sc.broadcast(list)

    // country, device, userId
    val rdd  = sc.parallelize(Seq(
      ("CHN", "iphone", 1),
      ("CHN", "iphone", 2),
      ("CHN", "mac", 1),
      ("USA", "ipad", 2),
      ("CAN", "mac", 3)))

    val rdd1 = sc.parallelize(Seq(
      ("CHN", "iphone", 1),
      ("CHN", "iphone", 2),
      ("CHN", "mac", 1),
      ("USA", "ipad", 2),
      ("CAN", "mac", 4)))



    val matchedRdd: RDD[(String, Int)] = rdd.flatMap(e => {
      val key = e._1 + "_" + e._2
      if(allPossiblities.value.contains(key)){
        Seq((key, e._3))
      }
      else
        Nil
    }).distinct().groupByKey().map(x => {
      (x._1, x._2.size)
      }
    )


    val rdd2 = sc.parallelize(Seq((5,"hello"),(2,"hello"),(3,"hello")))

    val rdd3 = sc.parallelize(Seq((1,"hello"),(2,"hello"),(3,"hello")))

    val joinrdd: RDD[(Int, (String, String))] = rdd2.join(rdd3)

    val tf = (x : Tuple2[Int, String]) => { x._1}

    rdd2.sortBy[Int]((x : Tuple2[Int, String]) => { x._1}, true, 10).collect().foreach( (x) => println(x._1 + " " +  x._2))
    rdd2.sortBy[Int](tf, true, 10)

    // Count by key 返回map
    val cntByKey: collection.Map[Int, Long] = rdd2.countByKey()

    // Group by multiple columns
    // records.groupBy(record => (record.column1, record.column2, record.column3))
    matchedRdd.foreach(x => println(x.toString()))
    matchedRdd.collect()

    rdd.union(rdd1)
    println("Test Intersection")
    rdd.intersection(rdd1).map( x => println(x._1 + " " + x._2 + " " + x._3)).collect()

  }
}
