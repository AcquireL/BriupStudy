package com.briup.demo.chap2
//使用递归完成Test6
object Test9 {
  def main(args: Array[String]): Unit = {
    println(function("Hello"))
    var s1:String="Hello"
    println(s1.charAt(0))
    println(s1.take(1))

  }
  def function(str:String): Long ={
    if(str.length==1) return str.charAt(0).toLong
    str.take(1).charAt(0).toLong * function(str.drop(1))
  }
}
