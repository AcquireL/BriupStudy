package com.briup.demo.chap4

import scala.collection.SortedMap
import scala.io.Source

object Test4 extends App {
  val source = Source.fromFile("Scala//src//main//scala//com//briup//demo//chap4//test1.txt").mkString
  val token = source.split("\\s+")
  var map= SortedMap[String, Int]()
  for (key <- token) {
    map += (key -> (map.getOrElse(key, 0) + 1))
  }
  map.foreach(println)
}
