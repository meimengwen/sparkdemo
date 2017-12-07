package com.mmw.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mmw on 2017/12/4.
  */
class HelloScala {

  def sayHello(username:String):Unit={
    println("hello"+username);

    val sparkconf = new SparkConf()
    sparkconf.setMaster("local")//设置运行模式为本地运行
    sparkconf.setAppName("HelloScala")
    val sc = new SparkContext(sparkconf)//构建连接器

    val input1 = sc.parallelize(List("handsom","stephen"))
    val input2 = sc.parallelize(List("beautiful","snow"))
    val unionlist = input1.union(input2)
    val interlist = input1.intersection(input2)
    val sublist = input1.subtract(input2)
    println(unionlist.collect().mkString(","))
    println(interlist.collect().mkString(","))
    println(sublist.collect().mkString(","))
  }


}
