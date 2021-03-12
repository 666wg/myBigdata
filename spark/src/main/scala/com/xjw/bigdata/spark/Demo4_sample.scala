package com.xjw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sample : 抽样，传入一个抽样比例
 */
object Demo4_sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sample").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    //从原来数据中随机抽取70%的数据，不是很准，只是概率问题,每次都不一样
    val rdd2: RDD[Int] = rdd1.sample(false,0.3)
    println("rdd2:"+rdd2.count())
  }
}
