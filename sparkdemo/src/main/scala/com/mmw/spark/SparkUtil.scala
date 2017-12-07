package com.mmw.spark

import org.apache.spark.{SparkConf,SparkContext}

/**
  * spark工具类
  * Created by mmw on 2017/12/4.
  */
object SparkUtil {
  /**
    * 获取本地模式sc对象
    * @return 获取到的sc
    */

  def getSc(isLocal:Boolean):SparkContext={
    val sparkconf = new SparkConf()
    if(isLocal){
      sparkconf.setMaster("local")
      sparkconf.set("spark.testing.memory", "2147480000")
    }else {
      sparkconf.setMaster("spark://master1.hadoop:7077")
      sparkconf.set("spark.testing.memory", "2147480000")
    }
    sparkconf.setAppName("SparkDemo")
    val sc = new SparkContext(sparkconf)
    if (!isLocal){
      sc.addJar("/home/hadoop/sparkdemo.jar")
    }
    return sc
    /**
      * 参数说明
      *setMaster:设置集群URL，告诉Spark如何连接到集群上，此处我们是用的是local
      * 这个特殊值可以让Spark运行在单机线程上而无需连接到集群上
      * setAppName:设置应用名，当连接到一个集群时，这个值可以帮助我们在集群管理器的用户界面中
      * 找到这个应用
      */
  }
}
