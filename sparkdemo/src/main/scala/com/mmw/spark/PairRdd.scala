package com.mmw.spark

/**
  * Created by mmw on 2017/12/6.
  */
/**
  * 键值对RDD演示示例
  * Created by Student on 2017/12/6.
  */
object PairRdd {

  def main(args:Array[String]): Unit ={
    PairRdd.mapDemo()
  }

  def mapCreatePairRdd(): Unit ={
    SparkUtil.getSc(true).textFile("README.md").map(x=>(x.split(" ")(0),x)).foreach(println)
  }

  def filterPairRdd(): Unit ={
    val rdd = SparkUtil.getSc(true).textFile("henushen.log").map(x=>(x.split(" ")(0),x)).filter{case(x,y)=>y.length()<20}
    rdd.cache()
    println(rdd.count())
    rdd.foreach(println)
    rdd.unpersist()
  }

  def mapValuesPairRdd(): Unit ={
    val sourceRdd = SparkUtil.getSc(true).textFile("henushen.log").map(x=>(x.split(" ")(0),x))
    val newRdd = sourceRdd.mapValues(value=>"aaa"+value)
    newRdd.foreach(println)
  }

  def wordCount(): Unit ={
    val sourceRdd = SparkUtil.getSc(false).textFile("henushen.log")
    val newRdd = sourceRdd.flatMap(x=>x.split(" ")).map(x=>(x,1))
    val words = newRdd.reduceByKey((x,y)=>x+y)
    words.foreach(println)
  }

  def wordCountPlus(): Unit ={
    val sourceRdd = SparkUtil.getSc(true).textFile("henushen.log")
    val newRdd = sourceRdd.flatMap(x=>x.split(" ")).countByValue()
    newRdd.foreach(println)
  }

  def combineByKey(): Unit ={
    val sourceRdd = SparkUtil.getSc(false).parallelize(List(("qwe",1),("asd",2),("zxc",4)))
    val newRdd = sourceRdd.combineByKey(
      (v)=>(v,1),
      (acc:(Int,Int),v)=>(acc._1+v,acc._2+1),
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2)
    )
    val avgRdd = newRdd.map{case(x,y)=>(x,y._1/y._2.toFloat)}
    println(avgRdd.collectAsMap().mkString(","))
  }

  def joinDemo():Unit={
    val sc = SparkUtil.getSc(true);
    val sourceRdd1 = sc.parallelize(List(("tiedan","45kg"),("pangdun","55kg"),("ndjb","60kg"),("dsa","70kg")))
    val sourceRdd2 = sc.parallelize(List(("tiedan","155cm"),("pangdun","160kg")))
    val joinRdd = sourceRdd1.join(sourceRdd2)
    joinRdd.foreach(println)
  }

  def leftjoinDemo():Unit={
    val sc = SparkUtil.getSc(true);
    val sourceRdd1= sc.parallelize(List(("tiedan","45kg"),("pangdun","60kg"),("paodan","70kg")))
    val sourceRdd2 = sc.parallelize(List(("tiedan","155cm"),("pangdun","160kg")))
    val joinRdd = sourceRdd1.leftOuterJoin(sourceRdd2)
    joinRdd.foreach(println)
  }

  def groupByKeydemo():Unit={
    val sourceRdd = SparkUtil.getSc(true).parallelize(List("tiedan 45kg","pangdun 55kg","pangdun 60kg","tiedan 70kg"))
    val pairRdd = sourceRdd.map(line=>{val array = line.split(" ")
      (array(0),array(1))
    })
    val pairRdd2 = pairRdd.groupByKey()
    pairRdd2.foreach{case(key,value)=>
    println("key值:"+key)
    value.foreach(println)
    }
  }

  def sortByKey():Unit={
    val sourcePairRdd = SparkUtil.getSc(true).parallelize(List(("cc",32),("bb",32)))
    val sortRdd = sourcePairRdd.sortByKey(true)
    sortRdd.foreach(println)
  }

  def countByKey():Unit={
    val input = SparkUtil.getSc(true).parallelize(List((1,2),(3,4),(5,6)))
    val result1 = input.countByKey()
    result1.foreach(println)
    val  result2 = input.lookup(1)
    result2.foreach(println)
  }

  def countByKeyAndlookup():Unit={
    val input = SparkUtil.getSc(true).parallelize(List((1,2),(3,4),(3,6)))
    val rdd1 = input.countByKey()
    val rdd2 = input.lookup(1)
    rdd1.foreach(println)
    println("===============")
    rdd2.foreach(println)
  }

  def mapDemo():Unit={
    val sourceRdd = SparkUtil.getSc(true).parallelize(List("stephen pangdnu","stephen paodan","stephen shuitomg"))
    val resultRdd = sourceRdd.map(line=>line.split(" "))
    println("RDD行数："+resultRdd.count())
    val array1 = resultRdd.collect()

    for(temp1<-array1){
          for(temp2<-temp1){
            println(temp2+" ")
      }
      println()
    }
  }

  def flatMapDemo():Unit={
    val sourceRdd = SparkUtil.getSc(true).parallelize(List("stephen pangdun","stephen paodan","stephen shuitong"))
    val resultRdd = sourceRdd.flatMap(line=>line.split(" "))
    println("RDD行数:"+resultRdd.count())
    val array1=resultRdd.collect()
    for (temp1<-array1){
      println(temp1)
    }
  }

}
