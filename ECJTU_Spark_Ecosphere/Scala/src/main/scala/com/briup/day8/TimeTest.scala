package com.briup.day8

object TimeTest {
  def main(args: Array[String]): Unit = {
    //传值调用，参数如果是个表达式
    //表达式的结果计算出来，在运行函数
    delayed(time())
    //传名调用，函数参数如果是个表达式
    //调用时不计算表达式结果，而是把表达式整体传入
    // 函数体内使用到了参数，在进行计算
    println("-----------------------")
    var j:Int=0
    myWhile(j<10){
      println(j)
      j+=1;
    }

  }
  def myWhile(a: =>Boolean)(block: =>Unit):Unit={
    if(a){
      block
      myWhile(a)(block)
    }
  }
  def time():Long ={
    println("获取时间，单位为纳秒")
    System.nanoTime()
  }
  def delayed(t: Long):Long={
    println("在 delayed 方法内")
    println("参数：" +t)
    t
  }
  def delayed1(t: =>Long):Long={
    println("在 delayed 方法内")
    println("参数：" +t)
    t
  }
  //
  def delay1(t:()=>Long)={
    println("在 delay中调用")
    println("参数：":+t())
    t
  }
}
