package com.briup.demo.chap4

import java.io.File
import java.util.Scanner

import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object Test2 extends App {
  /*    val file:File=new File("Scala//src//main//scala//com//briup//demo//chap4//test1.txt")
      val scanner:Scanner=new Scanner(file)
      while(scanner.hasNext()){
        println(scanner.next())
      }*/

  val source: String = Source.fromFile("Scala//src//main//scala//com//briup//demo//chap4//test1.txt").mkString
  val token: Array[String] = source.split("\\s+")
  val map=new mutable.HashMap[String,Int]
  for(i<- token){
    map(i)=map.getOrElse(i,0)+1
  }

  map.foreach(println)
}
