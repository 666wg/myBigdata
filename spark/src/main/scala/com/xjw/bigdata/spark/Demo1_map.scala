package com.xjw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * map: 对rdd中的数据进行处理，传入一行返回一行， 数据总行数不变
 */
object Demo1_map {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("map").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(5,6,7,8,9,10))
    // 需求：将集合中的元素，奇数加一，偶数乘二
    val rdd2=rdd1.map(i=>i%2 match {
      case 1=>i+1
      case 0=>i*2
    })
    rdd2.foreach(println)
  }
}
