package com.jianqing.scala.netflix

import com.jianqing.netflix.TaskInterface
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by jianqing_sun on 11/2/17.
  */
class ScalaAnalyticsTask() extends TaskInterface{
  val outputpath = "/tmp/movieanalytics"
  override def init(): Unit = {

  }

  override def run(): Int = {
    println("Running Scala Analytics Task...")
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("Analytics Task")
      .getOrCreate()
    val df: DataFrame = sparkSession.sqlContext.read.json("/tmp/moviejson")
    df.createOrReplaceTempView("movie")

    sparkSession.sqlContext.sql("select * from movie order by box_office desc limit 2").write.format("json").save(outputpath)
    0
  }

  override def clean(): Unit = {
    println("Cleaning Scala Analytics Task...")
  }
}

