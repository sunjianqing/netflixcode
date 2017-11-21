package com.jianqing.scala.netflix

import org.apache.spark.sql.SparkSession

/**
  * Created by jianqingsun on 11/19/17.
  */
object UDFDemo extends App {
  override def main(args: Array[String]): Unit = {
    println("Start")

    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark UDF Demo")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    import sparkSession.implicits._
    val dataset = Seq((0, "hello"), (1, "world"), (2, "spark")).toDF("id", "text")

    val upper = (v: String) => {
      v.toUpperCase
    }

    import org.apache.spark.sql.functions.udf
    val upperUDF = udf(upper)

    /*
    +---+-----+-----+
    | id| text|upper|
    +---+-----+-----+
    |  0|hello|HELLO|
    |  1|world|WORLD|
    |  2|spark|SPARK|
    +---+-----+-----+
     */
    dataset.withColumn("upper", upperUDF('text)).show


    dataset.sort($"id".desc).show() // 因为有implicit, $"id" 会被转成ColumnName

    // Register UDF
    sparkSession.udf.register("myUpper", upper)

    /*
      You can query for available standard and user-defined functions using the Catalog interface (that is available through SparkSession.catalog attribute).

      +-------+--------+-----------+-----------------------------------------------+-----------+
      |name   |database|description|className                                      |isTemporary|
      +-------+--------+-----------+-----------------------------------------------+-----------+
      |myupper|null    |null       |null                                           |true       |
      |upper  |null    |null       |org.apache.spark.sql.catalyst.expressions.Upper|true       |
      +-------+--------+-----------+-----------------------------------------------+-----------+
     */
    sparkSession.catalog.listFunctions.filter('name like "%upper%").show(false)

    dataset.createOrReplaceTempView("tmptbl")
    sparkSession.sqlContext.sql("select id, myUpper(text) from tmptbl").show()

    sparkSession.range(1).show()
    println("End")

  }
}
