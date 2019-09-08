package com.briup.demo.chap7

/*
     编写一段程序,将Java哈希映射中的所有元素拷贝到Scala哈希映射。用引入语句重命名这两个类。
 */

import java.util.{HashMap => JavaHashMap}

import scala.collection.mutable

object Test6 extends App {
  val map = new JavaHashMap[String, String]()
  map.put("1", "test1")
  map.put("2", "test2")
  map.put("3", "test3")

  val smap =new mutable.HashMap[String,String]()

  for (key <- map.keySet().toArray) {
    smap += (key.toString -> map.get(key))
  }
  smap.foreach(println)

}
