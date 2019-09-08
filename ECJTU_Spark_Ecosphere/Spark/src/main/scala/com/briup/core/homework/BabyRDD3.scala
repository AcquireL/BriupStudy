package com.briup.core.homework

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/*
  3、名字叫做John,Harry,Mary,Marilyn婴儿的出生趋势
 */
object BabyRDD3 extends App {
  val conf = new SparkConf().setMaster("local[1]").setAppName("lwj_first")

  val sc = new SparkContext(conf)

  val rdd1: RDD[(String, String)] = sc.wholeTextFiles("Spark/files/ssa_names").map(x => (x._1.substring(x._1.lastIndexOf("/") + 1, x._1.length), x._2))

  val rdd2 = rdd1.flatMap(line => {
    val arr = line._2.split("\n");
    val info = {
      arr.filter(x => x.contains("John,") || x.contains("Harry,") || x.contains("Mary,") || x.contains("Marilyn,")).map(x => {
        val a = x.split(",");
        ((a(0), line._1), a(2).trim.toInt);
      })
    };
    info
  });
  val rdd3 = rdd2.reduceByKey(_ + _).sortByKey(true)
  rdd3.saveAsTextFile("Spark/files/result_3")
  rdd3.foreach(println)
}
