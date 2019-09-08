package com.briup.day9

object ImplicaitTest {
  def main(args: Array[String]): Unit = {
    //隐式方法参数只能有一个
    implicit def in2String(i:Int)=i.toString


    def test(a:String,b:String): String ={
      a+b+a+b
    }
    val x:Int = 10
    x.length
    x.split("")
    println(x.concat("hello"))

    println(test(1,2))
  }
}
