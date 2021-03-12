package com.xjw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayOps

/**
 * flatMap: 将数据展开， 传入一行返回多行
 */
object Demo2_flatMap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("flatMap").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(List("hello,java,scala,spark","hello,java,scala"))
    rdd1.flatMap(line=>{
      val array: ArrayOps[String] = line.split(",").filter(word=>{!("java".equals(word))})
      array
    }).foreach(println)
  }
}
