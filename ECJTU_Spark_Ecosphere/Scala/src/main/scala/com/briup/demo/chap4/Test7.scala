package com.briup.demo.chap4

import java.util
import scala.collection.JavaConversions.propertiesAsScalaMap

/*
    打印java系统属性的表格
 */
object Test7 extends App{
  val props :scala.collection.Map[String,String]=System.getProperties()
  val keys: collection.Set[String] =props.keySet
  val keyLengths: collection.Set[Int] = for(key<-keys) yield key.length
  val maxLength=keyLengths.max
  for(key<- keys){
    print(key)
    print(" "*(maxLength-key.length) )
    print(" | ")
    println(props(key))
  }
}
