package com.jianqing.scala.netflix

/**
  * Created by jianqing_sun on 11/30/17.
  */
object FunctionalProgramming {
  def main(args: Array[String]): Unit = {

    /**
      * 1.函数和变量一样作为scala语言的一等公民，函数可以直接赋值给变量
      */
    val hiData = hiBigData _
    hiData("Spark")

    /**
      * 2.函数更常用的方式是匿名函数，定义的时候只需要说明输入参数的类型和函数体即可，不需要名称，但是如果要是使用的话，一般会把这个匿名函数
      * 赋值给一个val常量，spark源码中大量存在这种语法
      */
    val f = (name: String) => println("Hi " + name)
    f("Kafka")

    /**
      * 3.函数可以作为参数，直接传递给函数，这极大的简化了编程的语法，因为：
      *   第一：以前Java的方式是new出一个接口实例，并且在接口实例的回调方法中来实现业务逻辑，现在是直接把回调方法callback传递给函数，
      *        且在函数体中直接使用，简化了代码，提升了开发效率
      *   第二：这种方式非常方便编写复杂逻辑和控制逻辑，对于图计算、机器学习、深度学习等而言至关重要
      */
    def getName(func:(String) => Unit, name:String): Unit ={
      func(name)
    }
    getName(f,"Scala")

    Array(1 to 10:_*).map{(item:Int) => 2 * item}.foreach(x => println(x))

    /**
      * 4.函数式编程一个非常强大的地方之一：函数的返回值可以是函数，当函数的返回类型是函数的时候，这个时候就表明scala的函数实现了闭包
      *   scala闭包的内幕是：scala函数的背后是类和对象，所以scala的参数都作为了对象的成员，所以后续可以继续访问，这既是scala实现闭包原理
      *   的内幕
      */
    def funcResult(message:String) = (name: String) => println(message + " : " + name)

    funcResult("Hello")("Java")//Currying函数的写法，只要是复杂的scala函数编程都会采用这种写法
    //    等同于
    //    val result = funcResult("Hello")
    //    result("Java")

    //    非Currying写法
    //    def funcResult(message:String,name:String){
    //      println(message + " : " + name)
    //     }

  }

  def hiBigData(name: String): Unit = {
    println("Hi " + name)
  }

  def callFunc(func: (String, String) => String, otherParam: String): Unit = {
    func + otherParam
  }

}