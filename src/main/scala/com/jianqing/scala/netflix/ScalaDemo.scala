package com.jianqing.scala.netflix

/**
  * Created by jianqing_sun on 12/11/17.
  */
object ScalaDemo extends App {
  override def main(args: Array[String]): Unit = {
    val sd = new ScalaDemo("hello world")
    sd.parse()
  }

}

class ScalaDemo(input: String) {
  def parse(): Unit = {
    println(input)
  }
}