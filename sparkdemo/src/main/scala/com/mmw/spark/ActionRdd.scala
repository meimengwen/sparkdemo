package com.mmw.spark

import org.apache.spark.storage.StorageLevel

/**
  * RDD行动操作演示事例
  * Created by mmw on 2017/12/5.
  */
object ActionRdd {

  def main(args:Array[String]): Unit ={
    ActionRdd.aggregateDemo()
  }

  def reduceTest(x:Int,y:Int):Int={
    println(x)
    println(y)
    println(("-----"))
    x+y
  }

  def reduceDemo():Unit={
    val sourceRdd = SparkUtil.getSc(true).parallelize(List(1,2,3,4))
    val dataRdd = sourceRdd.reduce((x,y)=>(x+y))
    println(dataRdd)
  }

  //有时我们确实需要返回一个不同类型的值
  //使用aggregate时 需要提供我们期待返回的类型的初始值 然后通过一个函数把RDD中的元素合并起来放入累加器
  //因为到每个节点是在本地进行累加的 所以 还需要提供第二个函数来将累加器两两合并
  def aggregateDemo():Unit={
    val sourceRdd = SparkUtil.getSc(true).parallelize(List(1,2,3,4))
    val newRdd = sourceRdd.aggregate((0,0))(
      (acc,value)=>(acc._1+value,acc._2+1),
      (acc1,acc2)=>(acc1._1+acc2._1,acc1._2+acc2._2)
    )
    println(newRdd._1/newRdd._2)
  }

  def getData():Unit={
    val sourceRdd = SparkUtil.getSc(true).textFile("README.md")
    val collectArray = sourceRdd.collect()
    val takeArray = sourceRdd.take(5)
    val topArray = sourceRdd.top(5)
    println(collectArray.mkString(","))
    println(takeArray.mkString(","))
    println(topArray.mkString(","))
  }

  def foreachDemo():Unit={
    val sourceRdd = SparkUtil.getSc(true).textFile("README.md")
    println(sourceRdd.count())
    sourceRdd.foreach(x=>println(x))
  }

  //
  def cacheDemo():Unit={
    val sourceRdd = SparkUtil.getSc(true).textFile("README.md")
    val cacheRdd = sourceRdd.cache() //等同于persist
    sourceRdd.persist()//默认只用内存
    sourceRdd.persist(StorageLevel.MEMORY_ONLY)//等同于persist
    sourceRdd.persist(StorageLevel.MEMORY_AND_DISK_2)//内存磁盘混用 不推荐
    sourceRdd.persist(StorageLevel.DISK_ONLY)//纯磁盘 不推荐
  }
}

//RDD resillient distributed dataset 弹性分布式数据集
//它是Spark的最基本抽象 也是最核心的组件 是对分布式存储（内存 磁盘）的抽象使用，实现了以操作本地集合的方式来操作分布式数据集的抽象实现
//spark 兼容Iterative Algorithms,Relational Queries,MapReduce,Stream Processing.RDD混合了这四种模型 使得spark可以用于各种大数据处理场景
//RDD表示已被分区 不可变的并能够被并行操作的数据集合 不同的数据集格式对应不同的RDD实现 RDD必须是可序列化的


//spark如何使用内存做中间存储
//RDD可以cache到内存中，每次对RDD数据集的操作之后的结果，都可以存放到内存中
//下一个操作可以直接从内存中输入，省去了mapreduce 大量的磁盘IO操作 这对于迭代运算比较常见的机器学习算法，交互设计数据挖掘来说，效率提升非常大