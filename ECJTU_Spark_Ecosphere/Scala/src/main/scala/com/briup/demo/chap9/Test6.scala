package com.briup.demo.chap9

import scala.io.Source
import scala.util.matching.Regex

object Test6 extends App {
  val source: String =Source.fromFile("Scala\\src\\main\\scala\\com\\briup\\demo\\chap9\\Test6.txt").mkString

  val pattern: Regex ="\\w+\\s+\"".r

  println(source)
  pattern.findAllIn(source).foreach(println)



}
