package com.jianqing.scala.netflix

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by jianqing_sun on 11/6/17.
  *
  * How to test
  *
  * nc -lk 9999
  */
object NetworkWordCount {

  def main(args: Array[String]) {
    // Spark Streaming程序以StreamingContext为起点，其内部维持了一个SparkContext的实例。
    // 这里我们创建一个带有两个本地线程的StreamingContext，并设置批处理间隔为1秒。
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))
    // 在一个Spark应用中默认只允许有一个SparkContext，默认地spark-shell已经为我们创建好了
    // SparkContext，名为sc。因此在spark-shell中应该以下述方式创建StreamingContext，以
    // 避免创建再次创建SparkContext而引起错误：
    // val ssc = new StreamingContext(sc, Seconds(1))

    // 创建一个从TCP连接获取流数据的DStream，其每条记录是一行文本
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 对DStream进行转换，最终得到计算结果
    val res: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    // 打印该DStream中每个RDD中的前十个元素
    res.print()

    // 执行完上面代码，Spark Streaming并没有真正开始处理数据，而只是记录需在数据上执行的操作。
    // 当我们设置好所有需要在数据上执行的操作以后，我们就可以开始真正地处理数据了。如下：
    ssc.start()                     // 开始计算
    ssc.awaitTermination()          // 等待计算终止
  }
}
