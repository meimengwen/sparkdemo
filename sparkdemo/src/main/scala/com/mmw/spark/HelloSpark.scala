package com.mmw.spark

import org.apache.spark.{SparkConf,SparkContext}

/**
  * Spark 演示示例
  * Created by mmw on 2017/12/5.
  */
class HelloSpark {

  def demo1():Unit={
    val sparkconf = new SparkConf()
    //sparkconf.setMaster("local") //设置本地运行模式
    sparkconf.setMaster("spark://master1.hadoop:7077")//设置集群运行
    sparkconf.setAppName("HeloSpark")//设置运行程序名称
    sparkconf.set("spark.testing.memory","2147480000")//加大jvm内存
    val sc = new SparkContext(sparkconf)//构建连接器

    val testFileRdd = sc.textFile("hdfs://master1.hadoop:9000/spark/README.md")
    println(testFileRdd.count())
    sc.stop()

  }
}
