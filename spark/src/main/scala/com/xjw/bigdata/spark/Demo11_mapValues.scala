package com.xjw.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * mapvalue ； 处理value, key不变
 * 只限key value格式
 */
object Demo11_mapValues {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("mapValues").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String,  Int)] = sc.parallelize(List(("001",23),("003",18),("002",20)))
    // value+2
    rdd1.mapValues(age=>age+2).foreach(println)

  }

}
