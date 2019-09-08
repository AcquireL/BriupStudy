package com.briup.demo.chap4

object Test8 extends App {
  def minmax(arr:Array[Int]):Tuple2[Int,Int]={
    val min=arr.min
    val max=arr.max
    (min,max)
  }
  val arr=Array(2,3,5,7,0,9)
 println( minmax(arr))

}
