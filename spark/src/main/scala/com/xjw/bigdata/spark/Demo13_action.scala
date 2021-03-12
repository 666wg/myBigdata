package com.xjw.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * action算子, 每一个action算子都会触发一个job任务,job任务可以通过网页浏览，运行结束就会消失
 * foreach :遍历数据
 * count ;统计行数
 * reduce;全局聚合，有预聚合
 * save: 保存数据
 *      saveAsObjectFile：序列化输出
 *      saveAsTextFile：普通文本
 */
object Demo13_action {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("action").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(1,4,6,8,9))
    rdd1.foreach(println)
    val num: Long = rdd1.count()
    println("一共多少个元素"+num)
    val sum: Int = rdd1.reduce((x,y)=>{x+y})
    println("总和是多少"+sum)

    //        rdd1.saveAsTextFile("spark/data/out3")
    //        // 序列化输出
    //        rdd1.saveAsObjectFile("spark/data/out4")


  }

}

