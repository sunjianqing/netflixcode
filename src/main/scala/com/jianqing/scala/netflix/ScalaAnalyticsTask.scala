package com.jianqing.scala.netflix

import com.jianqing.netflix.TaskInterface
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianqing_sun on 11/2/17.
  */
class ScalaAnalyticsTask() extends TaskInterface{
  override def init(): Unit = {

  }

  override def run(): Int = {
    println("Running Scala Analytics Task...")
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("Analytics Task")
      .getOrCreate()
    val df = sparkSession.sqlContext.read.json("/tmp/moviejson")
    df.createOrReplaceTempView("movie")

    sparkSession.sqlContext.sql("select * from movie order by box_office desc limit 2").collect.foreach(println)
    0
  }

  override def clean(): Unit = {
    println("Cleaning Scala Analytics Task...")
  }
}

