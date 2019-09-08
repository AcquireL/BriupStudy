package com.briup.demo.chap2
//使用另一种方法实现6
object Test7 {
  def main(args: Array[String]): Unit = {
    function("Hello")
  }
  def function(num:String): Unit ={
    var sub:Long = 1
    num.foreach(sub*= _.toLong)
    print(sub)
  }
}
