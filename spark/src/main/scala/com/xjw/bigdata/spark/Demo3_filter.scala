package com.xjw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * filter: 过滤数据，函数返回true 保留数据， 函数返回false 过滤数据
 */
object Demo3_filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("filter").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6))
    //取出奇数
    val rdd2: RDD[Int] = rdd1.filter(i=>i%2==1)
    rdd2.foreach(println)
    //取出偶数
    val rdd3: RDD[Int] = rdd1.filter(_%2==0)
    rdd3.foreach(println)

  }
}
