package com.xjw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * union: 将两个rdd合并成一个rdd ， 两个rdd的类型必须一样
 * 合并rdd本质上是在逻辑层面，物理层面没有合并
 */
object Demo8_union {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("union").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(2,4,6,8))
    val rdd2: RDD[Int] = sc.parallelize(List(1,3,5,7))

    rdd1.union(rdd2).foreach(println)
  }

}
