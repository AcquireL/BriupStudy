package com.briup.core.homework

import org.apache.spark.rdd.RDD

/*
    5、按性别假定每年有频率最高的前N个名字(N最小)的婴儿人数总和超过当年该性别总人数的50%，
    N值随年份的变化趋势。（名字多样化分析）
 */
object BabyRDD5 extends App {
  val sc=getSC("babyRDD5")
  val rdd1: RDD[(String, String)] =sc.wholeTextFiles("Spark/files/ssa_names").map(x=>(x._1.substring(x._1.lastIndexOf("b")+1,x._1.length-4),x._2))

  val rdd2=rdd1.flatMap(line=>{
    val a=line._2.split("\n")
    var Fcount=0
    var Mcount=0
    val info=a.map(x=>{
      val b=x.split(",")
      b(1) match {
        case "F" => Fcount+=b(2).trim.toInt
        case "M" => Mcount+=b(2).trim.toInt
      }
      (line._1,b(1),b(2).trim.toInt)
    }).map(x=>{
        x._2 match {
          case "F" => ((x._1,x._2),(x._3,Fcount))
          case "M" => ((x._1,x._2),(x._3,Mcount ))
        }
    })
    info
  })
  val rdd3=rdd2.groupByKey().mapValues(arr=>{
    val num=0
    val count=0

  })
  rdd3.saveAsTextFile("Spark/result/result5")
  rdd3.foreach(println)
    //val rdd3=rdd2.

}
