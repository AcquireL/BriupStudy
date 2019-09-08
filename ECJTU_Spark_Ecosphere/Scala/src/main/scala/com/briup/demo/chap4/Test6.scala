package com.briup.demo.chap4

import java.util.Calendar

import scala.collection.mutable



object Test6 extends App{
  var linkedHashMap=mutable.LinkedHashMap[String,Int]()
  linkedHashMap+=("Monday" -> Calendar.MONDAY)
  linkedHashMap+=("Tuesday" -> Calendar.TUESDAY)
  linkedHashMap+=("Wednesday" -> Calendar.WEDNESDAY)
  linkedHashMap+=("Thursdat" -> Calendar.THURSDAY)
  linkedHashMap+=("Friday" -> Calendar.FRIDAY)
  linkedHashMap+=("Saturday" -> Calendar.SATURDAY)
  linkedHashMap.foreach(println)
}
