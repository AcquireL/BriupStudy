package com.briup.demo.chap3

/*
     编写一段代码，将a设置为一个n个随机整数的数组，要求随机数介于0和n之间。
 */
object Test1 extends App {
  def function(n:Int): Array[Int] ={
    val a=new Array[Int](n)
    val rand=new scala.util.Random()
    for(i<- a) yield {
      a(i)=rand.nextInt(n)
      a(i)
    }
  }
  function(9).foreach(println)
}
