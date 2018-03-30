package com.jianqing.scala.netflix

import org.apache.spark.sql.SparkSession

/**
  * Created by jianqingsun on 3/29/18.
  */
object ParquetDemo extends App {
  override def main(args: Array[String]): Unit = {
    val mySpark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val sc = mySpark.sparkContext

    // country, device, userId
    val rdd = sc.parallelize(Seq(
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


    rdd.intersection(rdd1).map(x => println(x._1 + " " + x._2 + " " + x._3)).collect()

  }
}
