package com.jianqing.scala.netflix

import org.apache.spark.sql.SparkSession

/**
  * Created by jianqing_sun on 11/27/17.
  */
object JsonDemo extends App {
  override def main(args: Array[String]): Unit = {
    println("Start")

    val file_path = "src/test/resource/json/testdata.json"
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark JSON Demo")
      .getOrCreate()

    val sqlContext = sparkSession.sqlContext
    val df = sqlContext.read.json(file_path)
    df.show()

    df.withColumn("name_alias", df.col("name")).select(col = "name_alias").show(1)

  }
}
