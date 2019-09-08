package com.briup.day3

import scala.collection.mutable.ArrayBuffer

object ArrayBufferTest extends App {
  //1 定义
  val ab1 = new ArrayBuffer[String]()
  val ab2= ArrayBuffer[String]("Hello","world")
  val ab3= ArrayBuffer.empty[String]
  //2 访问
  println("ab2 :"+ab2(0))
  //3 遍历
  //和定常数组一致
  //4 方法
  // ++= ++=: +: += +=: append(+:) drop rempve insert
  //sort
  //在原本数组上删除数据，返回值是删掉的数据
  //参数是要删除的数据下标
  val ab4=ab2.remove(1)  //返回remove的元素
  println(ab4==ab2)
  ab2.foreach(println)
  println("---------------")

  //删除原数据上数据返回一个新数组
  //参数从左往右删除的个数
  ab2.append("jack")
  ab2.append("adf")
  ab2.foreach(println)
  println("-----------------")
  val ab5=ab2.drop(2)
  println(ab5==ab2)
  ab5.foreach(println)
  println("------------------")
  ab2.foreach(println)
}
