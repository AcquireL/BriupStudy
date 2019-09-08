package com.briup.demo

object Test {
  def main(args: Array[String]) {
    val map = Map((1 -> "hello"), (2 -> "briup"))
    map.foreach(t => println(t._1+" " +t._2))
    println("------------------")
    val map1 = Map((3,"world"))
    val map3=map ++: map1
    val map4=map ++ map1
    map3.foreach(println)
    println("----------------")
    map4.foreach(println)
    println("----------------")
    //测试flatmap的用法
    Map("a" -> List(11,111), "b" -> List(22,222)).flatMap(_._2).foreach(println)

    println("----------------")
    Map("a" -> List(11,111), "b" -> List(22,222)).flatMap(x=>x._1).foreach(println)

    println("----------------")
   // Map("a" -> List(1 -> 11,1 -> 111), "b" -> List(2 -> 22,2 -> 222)).flatMap(_._2).foreach(println)

  }
}
