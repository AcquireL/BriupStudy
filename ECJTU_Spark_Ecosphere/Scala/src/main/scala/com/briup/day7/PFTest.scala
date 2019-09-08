package com.briup.day7

object PFTest {
  def main(args: Array[String]): Unit = {
    //偏函数接收变量必需声明类型
    val pf:PartialFunction[Int,String]={
      case 1 => "one"
      case 2 => "two"
      case 3 => "three"
      case _ => "other"
    }
    println(pf(2))
    println(pf(10))
    println("-----------------------")

    val v = Vector(1,2,3,4,5)
    val v_pf:Vector[String] = v.collect(pf)
    v_pf.foreach(println)
    println("-----------------------")

    val v_m = v.map( x => pf(x))

    println(pf.isDefinedAt(10))

    //匹配不到，用"hello"替代
    println(pf.applyOrElse(10,{num:Int => "hello"}))


  }


}
