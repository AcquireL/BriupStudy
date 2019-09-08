package com.briup.core.homework

import org.apache.spark.{SparkConf, SparkContext}

object BabyRDD1 extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("lwj_first")
  val sc = new SparkContext(conf);
  val source = sc.wholeTextFiles("Spark/files/ssa_names").map(x => (x._1.substring(x._1.lastIndexOf("b") + 1, x._1.length - 4), x._2))

  val result = source.flatMap(file => {
    val lines: Array[String] = file._2.split("\n")
    val infos = lines.map(x => {
      val info = x.split(",")
      ((file._1, info(1)),info(2).trim.toInt)
    })
    infos
  })
  val f_result=result.filter(x=>x._1._2=="F").groupByKey().mapValues(x=>x.size).map(x=>(x._1._1,x._2))
  val m_result=result.filter(x=>x._1._2=="M").groupByKey().mapValues(x=>x.size).map(x=>(x._1._1,x._2))

  val join =f_result.join(m_result).map(x=>(x._1,(x._2._1/gcd(x._2._1,x._2._2),x._2._2/gcd(x._2._1,x._2._2)))).sortByKey()
  //val join =f_result.join(m_result).map(x=>(x._1,(x._2._1.toDouble/x._2._2))).sortByKey()
  join.foreach(println)
 // join.saveAsTextFile("Spark/result/result_1")

  def gcd(x: Int, y: Int): Int = {
    val temp = x % y
    if (temp == 0) {
      y
    } else {
      gcd(y, temp)
    }
  }
  }
