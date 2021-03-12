package com.xjw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * sort： 排序
 * sortBy： 指定一个排序的列
 * sortBykey : 通过key排序，默认升序
 * 可以传入参数ascending，默认不传是升序，降序，需将其值改为false
 */
object Demo10_sort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("r").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, String, Int)] = sc.parallelize(List(("001","zs",23),("003","ls",18),("002","ww",20),("003","zl",23)))
    //通过年龄降序
    rdd1.sortBy(kv=>{
      kv._3
    },false).foreach(println)

    //通过key升序,需将rdd1变为keyvalue格式
    val rdd2=rdd1.map(kv=>{
      (kv._1,kv)
    })
    rdd2.sortByKey().foreach(println)

    //二次排序，通过学号和年龄
    rdd1.sortBy(kv=>(kv._1,kv._3)).foreach(println)

  }
}
