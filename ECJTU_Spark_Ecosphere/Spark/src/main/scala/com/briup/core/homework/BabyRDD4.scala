package com.briup.core.homework

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
  4、按性别计算每年最常用的1000个名字人数占当年总出生人数的比例（名字多样化分析）
 */
object BabyRDD4 extends App {
  val conf = new SparkConf().setMaster("local[1]").setAppName("lwj_babyRdd4")
  val sc = new SparkContext(conf)
  val rdd1: RDD[(String, String)] = sc.wholeTextFiles("Spark/files/ssa_names").map(x => (x._1.substring(x._1.lastIndexOf("/") + 1, x._1.length), x._2))

  val rdd2 = rdd1.flatMap(line => {
    val arr: Array[String] = line._2.split("\n")
    var totalM = 0;
    var totalF = 0;
    val info = arr.map(x => {
      val a = x.split(",")
      a(1) match {
        case "F" => totalF += a(2).trim.toInt
        case "M" => totalM += a(2).trim.toInt
      }
      ((line._1, a(0)), (a(1), a(2).trim.toInt))
    }).map(x => {
      val f = gcd(x._2.x._2, totalF)
      val m = gcd(x._2.x._2, totalM)
      x._2._1 match {
        case "F" => ((x._1._1, x._1._2), (x._2._1, x._2._2 / f, totalF / f))
        case "M" => ((x._1._1, x._1._2), (x._2._1, x._2._2 / m, totalM / m))
      }
    })
    info
  })
  rdd2.take(1000).foreach(x=>{
    println("年份："+x._1._1+" 姓名："+x._1.x._2+" 性别："+x._2.x._1+" 比值"+x._2._2+":"+x._2._3)
  })

  def gcd(x: Int, y: Int): Int = {
    val temp = x % y
    if (temp == 0) {
      y
    } else {
      gcd(y, temp)
    }
  }
}
