package com.briup.day4

import scala.collection.mutable.ArrayBuffer

object SortTest {
  def main(args: Array[String]): Unit = {
    /*val ab=ArrayBuffer[Int](6,7,4,2,1,9,3,5,8)
    //按照容器内元素 自己的特性进行排序
    val ab1 = ab.sorted
    ab1.foreach(println)
    println("---------------------------")
    //按照某个比较规则进行排序
    val ab2 = ab.sortBy(i => i)
    ab2.foreach(println)
    println("---------------------------")
    //按照参数传入的比较函数进行排序
    val ab3 = ab.sortWith((i,j) => i>j)
    ab3.foreach(println)
    println("---------------------------")*/


    val s1 =new Student("jack",18,71.0)
    val s2=new Student("rose",20,72.0)
    val s3 =new Student("lily",28,75.0)
    val s4 =new Student("kali",23,73.0)
    val s5 =new Student("bob",24,74.0)
    val ab=ArrayBuffer[Student](s1,s2,s3,s4,s5)
    ab.foreach(println)
    println("==================================")
/*    val ab1=ab.sorted
    ab1.foreach(println)
    println("=================================")*/

    //sortBy 比较器怎么传 val s1 =new Student("jack",23,72.0)
    implicit  object StudentOrdering extends Ordering[Student]{
      override def compare(x: Student, y: Student): Int = (y.score-x.score).toInt
    }
   /* val ab2 = ab.sortBy(s => s)
    ab2.foreach(println)
    println("===================================")*/

    val ab3=ab.sortWith((s1,s2)=>s1.name.compareTo(s2.name)<0)
    ab3.foreach(println)
    println("=====================================")


  }
}
