package com.briup.demo.chap4

import scala.io.Source

object Test3 extends App {
  val source=Source.fromFile("Scala//src//main//scala//com//briup//demo//chap4//test1.txt").mkString
  val token=source.split("\\s+")
  var map: Map[String, Int] =Map[String,Int]()
  for(key<- token){
    map+=(key->(map.getOrElse(key,0)+1))
  }
  println(map.mkString(","))
  map.foreach(println)
}
