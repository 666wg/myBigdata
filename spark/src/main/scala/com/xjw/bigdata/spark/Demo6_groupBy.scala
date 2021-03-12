package com.xjw.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupby : 指定一个列来分组
 * 相比于groupbykey,将key换成
 */
object Demo6_groupBy {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("groupBy").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.parallelize(List((1,2),(2,3),(3,4),(3,6)))
    // 相同key是奇偶分组
    val result = rdd.groupBy(line=>{
      line._1 % 2
    })
    result.foreach(println)
  }
}
