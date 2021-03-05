package com.xjw.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Spark 的 WordCount
 */
object SparkWordCount {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("WordCount")
                              .setMaster("local")

    val sc = new SparkContext(conf)
    /* input.txt内容
      aaa bbb
      bbb ccc
      ddd
     */
    val test = sc.textFile("C:\\Users\\w\\Desktop\\input.txt")

    test.flatMap { line => line.split(" ") }
        .map { word => (word, 1) }
        .reduceByKey(_ + _)
        .saveAsTextFile("C:\\Users\\w\\Desktop\\output")
    /* 输出结果output目录下part-00000文件内容
      (bbb,2)
      (ddd,1)
      (ccc,1)
      (aaa,1)
     */

    sc.stop

  }

}
