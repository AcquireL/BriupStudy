package com.briup.day6

import java.util

import scala.collection.mutable

object ParTest {
  def main(args: Array[String]): Unit = {
    val r = 1 to 10
    r.foreach(println)
    println("-----------------------")
    r.par.foreach(println)
    (0 to 10000).par.collect{case _ =>Thread.currentThread.getName}.distinct.foreach(println)
    println("-----------------------")
    val nums = new util.ArrayList[Int]();
    nums.add(1)
    nums.add(2)
    nums.add(3)
    nums.add(4)
    nums.add(5)
    import scala.collection.JavaConverters._
    val snums: mutable.Seq[Int] = nums.asScala;
    import scala.math._
    snums.map(pow(_,2)).foreach(println)

  }
}
