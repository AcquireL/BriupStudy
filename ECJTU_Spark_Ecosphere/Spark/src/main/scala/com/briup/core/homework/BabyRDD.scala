package com.briup.core.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
  1、130年中每年出生婴儿的男女比例
 */
object BabyRDD extends App {
  val conf=new SparkConf().setMaster("local[1]").setAppName("lwj_first")

  val sc = new SparkContext(conf)

  val rdd1: RDD[(String, String)] =sc.wholeTextFiles("Spark/files/ssa_names").map(x=>(x._1.substring(x._1.lastIndexOf("/")+1,x._1.length),x._2))

  val rdd2: RDD[(String, Int)] =rdd1.flatMapValues(x=>(x.split(","))).filter(x=> x._2=="F" ).groupByKey().mapValues(x=>x.size)

  val rdd3: RDD[(String, Int)] =rdd1.flatMapValues(x=>(x.split(","))).filter(x=> x._2=="M" ).groupByKey().mapValues(x=>x.size)

  val join: RDD[(String, Double)] =rdd2.join(rdd3).mapValues(x=>Int.int2double(x._1)./(x._2)).sortByKey()

  val test: RDD[(String, String)] =rdd1.flatMapValues(x=>(x.split(","))).filter(x=> x._2=="M" )
  test.foreach(println)
  //join.foreach(println)


}
