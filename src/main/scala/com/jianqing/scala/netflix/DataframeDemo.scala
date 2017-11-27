package com.jianqing.scala.netflix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by jianqing_sun on 11/9/17.
  */
object DataframeDemo extends App{
  override def main(args: Array[String]): Unit = {

    val mySpark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .getOrCreate()

    val sc = mySpark.sparkContext

    val rdd1: RDD[(Int, String)] = sc.parallelize(Seq(
      (1, "hello"),
      (2, "world"),
      (3, "spark"),
      (4, "hello"),
      (5, "apple")
    ))

    val rdd2: RDD[(Int, String)] = sc.parallelize(Seq((1, "cat1"),(2, "cat2"),(33, "cat33")))

    val res = rdd1.fullOuterJoin(rdd2).map(e => {
      val joinKey = e._1
      val joinValue = "left " + e._2._1.getOrElse("NULL") + " right " + e._2._2.getOrElse("NULL")
      println(joinKey + " : " + joinValue)
    })

    res.collect()

    // For implicit conversions like converting RDDs to DataFrames
    import mySpark.implicits._

    val someDF = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-27, "horse")
    ).toDF("number", "word")

    someDF.select("number", "word").explain(true)

    someDF.select("number","word").show()

  }

}
