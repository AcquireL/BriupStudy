package com.briup.demo.chap4

object Test9 extends App {
  def Iteqgt(arr:Array[Int],v:Int)={
    (arr.count(_<v),arr.count(_==v),arr.count(_>v))
  }
  val array=Array(3,4,5,8,9)
  println(Iteqgt(array,5))
}
