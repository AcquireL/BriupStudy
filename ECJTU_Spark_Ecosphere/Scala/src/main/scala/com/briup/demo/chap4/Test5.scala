package com.briup.demo.chap4

import java.util.TreeMap
import scala.collection.{JavaConverters, mutable}
import scala.io.Source

object Test5 extends App {
  val source = Source.fromFile("Scala//src//main//scala//com//briup//demo//chap4//test1.txt").mkString
  val token = source.split("\\s+")
  var javaMap= new TreeMap[String, Int]()
  val map: mutable.Map[String, Int] =JavaConverters.mapAsScalaMap(javaMap)
  for (key <- token) {
    map += (key -> (map.getOrElse(key, 0) + 1))
  }
  map.foreach(println)

}
