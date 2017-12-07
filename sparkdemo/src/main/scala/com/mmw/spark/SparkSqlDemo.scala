package com.mmw.spark

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Sparksql组件演示示例
  * Created by captain on 2017/12/6.
  */
object SparkSqlDemo {

  def main(args:Array[String]){
    jdbcDemo()
  }

  def hiveDemo():Unit={
    val sparkSession = SparkSession.builder()
    sparkSession.master("local")
    sparkSession.appName("SparkSqlHiveDemo")
    sparkSession.enableHiveSupport()
    val hiveSession = sparkSession.getOrCreate()
    val rowsRdd:DataFrame = hiveSession.sql("SELECT * FROM record")
    val row = rowsRdd.first()
    println(row.getString(0))
  }

  def jsonDemo():Unit={
    val sparkSession = SparkSession.builder()
    sparkSession.master("local")
    sparkSession.appName("SparkSqlJsonDemo")
    val jsonSession = sparkSession.getOrCreate()
    val jsonRdd:DataFrame = jsonSession.read.json("jsondata.txt")
    jsonRdd.createOrReplaceTempView("adtable")
    jsonRdd.persist()
    val rows:DataFrame = jsonSession.sql("select * from adtable")
    val row = rows.first()
    println(row.getString(0))
    println(row.getString(1))
    println(row.getAs("title"))
    rows.show(10)
  }

  def jdbcDemo():Unit={
    val sparkSession = SparkSession.builder()
    sparkSession.master("local")
    sparkSession.appName("SparkSqlJdbcDemo")
    val jdbcSession = sparkSession.getOrCreate()
    val rows:DataFrame = jdbcSession.read.jdbc("jdbc:mysql://localhost:3306/student?characterEncoding=UTF-8&user=root&password=741852","t_students","sex",1,5,2,new Properties())
    rows.show(5)
  }

}
