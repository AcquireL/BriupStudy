package com.briup.day8

object FunctionTest {
  def main(args: Array[String]): Unit ={
    //1 字面量 String str = "Hello"
    val f: String => Int = (str:String) =>{str.length}
    val f1:Function[String,Int]= (str:String) =>{str.length}
    //2 Function0-22
    val f2: (String => Int) = new Function[String,Int] {
      override def apply(v1: String): Int = {
        v1.length
      }
    }
    //匿名函数, 传入高阶函数
    ((a:Int,b:Int)=>a+b)(10,20)
    //中缀函数有语法糖

    //函数的类型推断，作为高阶函数的参数
    import scala.math._
    (1 to 10).map((x:Int)=>{pow(x,2)})
    (1 to 10).map(x=>pow(x,2))
    //如果参数列表在{}只用了一次，则可以省略参数列表
    // 用_代替
    (1 to 10).map(pow(_,2))
    (1 to 10).reduce(_+_)
    (1 to 10).reduce(_*_)

    //手动ETA, 参数列表，返回值
    (1 to 10).foreach(println _)

    def showSize(str:String):Int={
      str.length
    }
    val a = Array[String]("Hello","bye","good")
    a.map((x:String) => {showSize(x)})
    a.map(showSize)
    println("----------------------------")
    println((1 to 10) reduce ((x: Int, y: Int) => x + y))
  }
}
