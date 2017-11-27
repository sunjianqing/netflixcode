package com.jianqing.scala.netflix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by jianqing_sun on 11/9/17.
  */
case class Movie(title: String, movieId: Int, userId: Int, device: Int)

object DataframeDemo extends App {
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

    val rdd2: RDD[(Int, String)] = sc.parallelize(
      Seq(
        (1, "cat1"),
        (2, "cat2"),
        (33, "cat33")))

    val res = rdd1.fullOuterJoin(rdd2).map((e: (Int, (Option[String], Option[String]))) => {
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

    someDF.select("number", "word").show()

    selfJoinDemo(mySpark)
  }

  def selfJoinDemo(sparkSession: SparkSession): Unit = {
    val sqlContext = sparkSession.sqlContext
    val mdf = sqlContext.createDataFrame(
      Movie("t1", 1001, 1, 1)
        :: Movie("t2", 1002, 1, 1)
        :: Movie("t3", 1003, 1, 1)
        :: Movie("t2", 1002, 2, 1)
        :: Movie("t3", 1003, 2, 1)
        :: Nil)

    val df1 = mdf.as("df1")
    val df2 = mdf.as("df2")

    // dataframe join 写法

    // 直接用df("columnname") 会返回 column
    df1.join(df2, df1("userId") === df2("userId"))

    import org.apache.spark.sql.functions.col
    // 或者用col() 函数
    val joinedData = df1.join(df2, col("df1.userId") === col("df2.userId"), "inner")
      .filter(col("df1.movieId") !== col("df2.movieId")) // Column format
      .filter("df1.userId == 1") // Expression format

    joinedData.explain(true)
    joinedData.show()

    leftJoinDemo(sparkSession)

  }

  def leftJoinDemo(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    val sc = sparkSession.sparkContext
    val employees = sc.parallelize(Seq[(String, Option[Int])](
      ("Rafferty", Some(31)),
      ("Jones", Some(33)),
      ("Heisenberg", Some(33)),
      ("Robinson", Some(34)),
      ("Smith", Some(34)),
      ("Williams", null)
    )).toDF("LastName", "DepartmentID")

    val departments = sc.parallelize(Seq(
      (31, "Sales"),
      (33, "Engineering"),
      (34, "Clerical"),
      (35, "Marketing")
    )).toDF("DepartmentID", "DepartmentName")

    val joinedData: DataFrame = employees.join(departments, Seq("DepartmentID"), "left_outer")

    joinedData.show()
    /*
    +------------+----------+--------------+
    |DepartmentID|  LastName|DepartmentName|
    +------------+----------+--------------+
    |          31|  Rafferty|         Sales|
    |          34|  Robinson|      Clerical|
    |          34|     Smith|      Clerical|
    |        null|  Williams|          null|
    |          33|     Jones|   Engineering|
    |          33|Heisenberg|   Engineering|
    +------------+----------+--------------+
     */

    joinedData.foreach( (r: Row) => {
      val firstValue = r(0)
      val secondValue = r.getAs[String](1)
      val thirdValue = r.getAs[String](2)

      println(firstValue + " " + secondValue + " " + thirdValue)
    })

  }

}
