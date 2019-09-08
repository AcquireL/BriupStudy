package com.briup.demo.chap3
import java.awt.datatransfer._
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.mutable.Buffer


import scala.collection.mutable
/*
    引入java.awt.datatransfer._并构建一个类型为SystemFlavorMap类型的对象，
    然后以DataFlavor.imageFlavor为参数调用getNativesForFlavor方法，以Scala缓冲保存返回值。
 */
object Test10 extends App{
  val flavors = SystemFlavorMap.getDefaultFlavorMap().asInstanceOf[SystemFlavorMap]
  val buf : mutable.Buffer[String] = flavors.getNativesForFlavor(DataFlavor.imageFlavor);
  buf.foreach(println);

}
