package com.jianqing.scala.netflix

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by jianqing_sun on 11/28/17.
  */
object PartitionDemo extends App {
  override def main(args: Array[String]): Unit = {
    //http://dev.sortable.com/spark-repartition/
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Partition Demo")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    // Compute Prime within 2 to n
    val n = 2000000

    sc.parallelize(2 to n, 4).foreachPartition((x) => {
      //println(x.size)
    })


    val composite = sc.parallelize(2 to n, 4).map(x => (x, (2 to (n / x))))
      .repartition(4) // 因为大部分数据会落到key 为 2 那个partition， 所有repartition 会有帮助
      .flatMap(kv => kv._2.map(_ * kv._1))
    //println(composite.partitioner)
    val prime = sc.parallelize(2 to n, 4).subtract(composite)

    prime.collect()

    // Range Partitioner
    val m = 100
    val pairRdd: RDD[(Int, Range.Inclusive)] = sc.parallelize(2 to m, 8).map(x => (x, (2 to (m / x))))

    val rpartitioner = new RangePartitioner[Int, Range.Inclusive](8, pairRdd)
    val newPartitionRdd = pairRdd.partitionBy(rpartitioner)
    println(newPartitionRdd.partitioner.get)
    newPartitionRdd.foreachPartition( (x) =>
      {
        println("=====================")
        x.foreach( (i) => print(i._1 + ","))
        println("")
        println("=====================")

      }
    )
  }
}
