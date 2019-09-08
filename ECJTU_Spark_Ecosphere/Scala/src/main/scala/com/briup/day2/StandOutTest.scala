package com.briup.day2

/*
import java.lang._
import scala.lang._
import scala.Predef._
*/
object StandOutTest extends App{
  //println print printf从哪里来的
  //从Predef对象中来
  //1 提供了很多别名
  //2 提供了静态方法 println
  //3 提供了隐式操作 Int --> RichInt
  println("hello world")

  val str = scala.io.StdIn.readLine()
  println(str)
}
