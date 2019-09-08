package com.briup.demo.chap3

/*
    编写一个循环，将整数数组中相邻的元素置换。
 */
object Test2 extends App {
  val a=Array[Int](1,2,3,4,5,6,7)
  //a.foreach(println)
  for(i<- 0 until (a.length - 1, 2)){
    var tmp:Int=a(i)
    a(i)=a(i+1)
    a(i+1)=tmp
  }
  a.foreach(println)
}
