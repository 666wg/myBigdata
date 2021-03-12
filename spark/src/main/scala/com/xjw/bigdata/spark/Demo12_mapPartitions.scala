package com.xjw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * mapPartition; 处理一个分区的数据
 * getNumPartitions :获取分区数， 不是一个算子
 * mapPartitionsWithIndex 标明分区编号
 */
object Demo12_mapPartitions {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("mapPartitions").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(1,3,5,6))
    val rdd2: RDD[Int] = sc.parallelize(List(2,4,5,6))
    val rdd3: RDD[Int] = rdd1.union(rdd2)

    //查看当前分区数量
    println("分区数量为"+s"${rdd3.getNumPartitions}")

    //对每个分区内得数据进行执行，返回得是一个迭代器，也可以带有分区编号
    rdd3.mapPartitionsWithIndex((index,iter)=>{
      println("分区编号："+index)
      iter.map(i=>{
        i*2
      })
    }).foreach(println)

    //不带分区编号,将每个分区内得元素，加1
    rdd3.mapPartitions(iter=>{
      iter.map(i=>{
        i+1
      })
    }).foreach(println)
  }

}
