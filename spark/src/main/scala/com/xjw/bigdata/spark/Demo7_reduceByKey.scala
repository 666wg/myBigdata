package com.xjw.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * reduceBykey: 通过key对value进行聚合，需要传入一个聚合函数，只能用于聚合，不能用于排序、分组，求平均
 * reduceByKey 效率比groupBykey 高 reduceBykey会在map端进行预聚合
 * 预聚合：将每个分区的的数据，先把相同的key的进行聚合
 * 能使用reduceBykey的时候就使用reduceBykey
 */
object Demo7_reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("reduceByKey").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.parallelize(List((1,2),(3,4),(3,6)))
    // 相同key的value值求和
    val result = rdd.reduceByKey((v1,v2)=>v1+v2)
    result.foreach(println)
  }
}
