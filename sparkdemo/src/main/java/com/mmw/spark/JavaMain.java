package com.mmw.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * scala-java工程混合调用演示事例起点类
 * Created by mmw on 2017/12/4.
 */
public class JavaMain {
    public  static  void  main(String[] args){
       /* HelloScala helloScala = new HelloScala();
        helloScala.sayHello("stephen");*/
        HelloSpark helloSpark = new HelloSpark();
        helloSpark.demo1();
    }
}
