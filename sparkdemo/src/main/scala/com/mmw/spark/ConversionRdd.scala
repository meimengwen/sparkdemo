package com.mmw.spark

import org.apache.spark.SparkContext
/**
  * RDD转化操作实例
  * Created by mmw on 2017/12/4.
  */
object ConversionRdd {

  def main(args: Array[String])= {
    ConversionRdd.filterDemo()
  }
  def filterDemo():Unit={
    val  sourceRdd = SparkUtil.getSc(true).textFile("henushen.log")
      val  warnRdd=sourceRdd.filter(line=>line.contains("WARN"))
    val  errorRdd=sourceRdd.filter(line=>line.contains("ERROR"))
    println(warnRdd.count())
    println(errorRdd.count())
  }
  def mapDemo():Unit={
    val sourceRdd=SparkUtil.getSc(true).parallelize(List(1,2,3,4))
    val doubleRdd=sourceRdd.map(x=>x*x)
    println(doubleRdd.collect().mkString(","))
    println(doubleRdd.count())
  }
  def flatMapDemo():Unit={
    val sourceRdd=SparkUtil.getSc(true).parallelize(List("qiaoEn big xiong","qiaoEn big tui"))
    val words=sourceRdd.flatMap(line=>line.split(" "))
    println(words.collect().mkString(","));
  }
  def  distinctDemo():Unit={
    val  sourceRdd=SparkUtil.getSc(true).parallelize(List("qiaoEn","qiaoEn","goudan"))
    val distinctRdd=sourceRdd.distinct()
    println(distinctRdd.collect().mkString(","))
  }

  def jbxDemo():Unit={
    val sc =SparkUtil.getSc(true)
    val sourceRdd1=sc.parallelize(List("girl","big","long"))
    val  sourceRdd2=sc.parallelize(List("boy","long","big"))
    val  unionRDD=sourceRdd1.union(sourceRdd2)
    val  interRdd=sourceRdd1.intersection(sourceRdd2)
    val subRdd=sourceRdd1.subtract(sourceRdd2)
    println(unionRDD.collect().mkString(","))
    println(interRdd.collect().mkString(","))
    println(subRdd.collect().mkString(","))
  }
}
