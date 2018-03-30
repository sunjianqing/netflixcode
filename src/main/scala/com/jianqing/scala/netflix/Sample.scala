package com.jianqing.scala.netflix

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext}

/**
  * Created by jianqing_sun on 11/5/17.
  */
class Sample {
  def run(): Unit = {
    val conf = new SparkConf();
    conf.setAppName("Sample")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Seq(
      ("key1", 1),
      ("key2", 2),
      ("key1", 3)))
    val partitions = rdd1.partitions
    println(partitions.length)
    val rdd2 = rdd1.repartition(2)
    val partitions2 = rdd2.partitions

    val rdd3: RDD[(String, Int)] = sc.parallelize(Seq(
      ("key1", 5),
      ("key2", 4)))


    partitions2.foreach((p: Partition) => println(p.getClass.getCanonicalName))

    val grouped = rdd1.cogroup(rdd3)
    grouped.collect()
    val updated = grouped.map { x => {
      val key = x._1
      println("Key -> " + key)
      val value = x._2
      val itl1 = value._1
      val itl2 = value._2
      val res1 = itl1.map { x => {
        println("It1 : Key -> " + key + ", Val -> " + x )
      }
      }
      val res2 = itl2.map { x => {
        println("It2 : Key -> " + key + ", Val -> " + x)
      }
      }
      println("End")
      (key, (res1, res2))
    }
    }
    updated.collect()


  }
}

object Sample extends App {
  override def main(args: Array[String]): Unit = {
    println("Hello world")
    val s = new Sample()
    s.run()
  }
}