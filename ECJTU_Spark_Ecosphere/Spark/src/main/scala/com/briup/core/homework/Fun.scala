package com.briup.core.homework

import com.briup.core.homework.BabyRDD5.sc
import org.apache.spark.rdd.RDD
import org.junit.Test

class Fun {
  @Test
  def map(): Unit ={
    val sc=getSC("map")
    val a: RDD[Int] =sc.parallelize(1 to 10,3)
    val b: RDD[(Int, Int)] =a.map(x=>(x,x*2))
    val b2: RDD[Int] =a.flatMap(x=>1 to x)
    a.foreach(println)
    println("--------------")
    b.foreach(println)
    println("--------------")
    b2.foreach(print)


    val test: RDD[String] =sc.parallelize(List("dog","tiger","lion","cat","panther","eagle"),2)
    test.map(x=>(x.length,x))
    val dd: RDD[Char] =test.flatMap(x=>x)

  }
}
