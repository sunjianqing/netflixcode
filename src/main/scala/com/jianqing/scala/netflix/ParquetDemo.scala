package com.jianqing.scala.netflix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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

    val schema: StructType = StructType(
      Array(
        StructField(name = "country", dataType = StringType, nullable = false),
        StructField(name = "type", dataType = StringType, nullable = false),
        StructField(name = "id", dataType = IntegerType, nullable = false))
    )

    // Create DataFrame from RDD
    val rowRdd: RDD[Row] = rdd.map(r => Row(r._1, r._2, r._3))
    val dataFrame = mySpark.createDataFrame(rowRdd, schema)


    // Write out as Parquet file
    val filePath = "src/test/resource/parquet/data.parquet"
    dataFrame.write.option("compression","none").mode("overwrite").parquet(filePath)
  }
}
