package com.xjw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * join: 连接，默认为内部连接
 * rightjoin以右表为准，没有匹配到返回none，可以设置默认值
 * leftjoin以左表为准，没有匹配到返回none，可以设置默认值
 */
object Demo9_join {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("join").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, String)] = sc.parallelize(List(("001","zs"),("002","ls"),("003","ww")))
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("001",13),("002",20),("004",18)))
    val rdd3: RDD[(String, (String, Int))] = rdd1.join(rdd2)
    rdd3.foreach(println)

    //以左表连接
    val rdd4: RDD[(String, (String, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    rdd4.map(kv=>{
      val key: String = kv._1
      val name: String = kv._2._1
      val age: Int = kv._2._2.getOrElse(0)
      (key,name,age)
    }).foreach(println)

  }

}

