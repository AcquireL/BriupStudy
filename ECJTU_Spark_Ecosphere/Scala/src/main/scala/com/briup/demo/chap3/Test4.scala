package com.briup.demo.chap3

import scala.collection.mutable.ArrayBuffer

/*
    给定一个整数数组，产出一个新的数组，包含元数组中的所有正值，
    以原有顺序排列，之后的元素是所有零或负值，以原有顺序排列。
 */
object Test4 extends App {
  val a:Array[Int] =Array[Int](4,5,-1,0,-5,2,-3,0)

  def function(array:Array[Int]):Array[Int]={
    val b=new ArrayBuffer[Int](a.length)
    b ++= (for ( i <- a if(i > 0)) yield  i)
    b ++= (for ( i <- a if(i==0)) yield i)
    b.++=(for (i <- a if(i<0)) yield i)
    b.toArray
  }
  val b=function(a)
  b.foreach(println)
}
