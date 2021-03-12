package com.xjw.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupByKey : 通过key进行分组，将相同的key分到同一个reduce中
 * 会产生shuffle
 */
object Demo5_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.parallelize(List((1,2),(3,4),(3,6)))
    // 相同key分组
    val result = rdd.groupByKey()
    result.foreach(println)
  }
}
