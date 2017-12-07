package com.mmw.spark

import org.apache.hadoop.io.{IntWritable, Text}

/**
  * 文件的读取保存演示示例
  * Created by captain on 2017/12/7.
  */
object FileReadAndWriteDemo {

  def main(args:Array[String]){
    sequenceFileWrite()
  }

  def txtFileRead():Unit={
    val sc = SparkUtil.getSc(true)
    val sourceRdd = sc.textFile("D:\\logs")
    val pairRdd = sc.wholeTextFiles("D:\\logs")
    sourceRdd.foreach(println)
    println("=========================================")
    pairRdd.foreach(println)
  }

  def txtFileWrite():Unit={
    val sc = SparkUtil.getSc(true)
    val sourceRdd = sc.textFile("D:\\logs\\readme1.txt")
    sourceRdd.saveAsTextFile("D:\\logs\\output1")
    val sourceRdd2 = sc.parallelize(List((1,2),(3,4),(5,6)))
    sourceRdd2.saveAsTextFile("D:\\logs\\output2")
  }

  def sequenceFileRead():Unit={
    val sourceRdd = SparkUtil.getSc(true).sequenceFile("part-00000",classOf[Text],classOf[IntWritable])
    sourceRdd.foreach(println)
  }

  def sequenceFileWrite():Unit={
    val sourcePairRdd = SparkUtil.getSc(true).parallelize(List(("Panda",32),("Kay",32),("Snail",22)))
    sourcePairRdd.saveAsSequenceFile("sequenceData")
  }

}
