package com.briup.day4

import scala.collection.mutable
object MapTest {
  def main(args: Array[String]): Unit = {
    //1 定义map
    //可变map 和不可变的map
    val m1=Map((1,"hello"),(2,"world"),(3,"briup"))
    val m2=Map((1->"hello"),(2->"world"),(3->"briup"))
    val m3=mutable.Map((1,"hello"))


    //2 获取map中数据
    // m1(key) m1.get() m1.getOrElse()
    val v1 = m1(2)

   //Option是为了解决空指针异常而产生的类
    //get方法返回的值 ， Some  None
    val v2: String =m1.get(1) match {
      case a:Some[String] => a.get
      case None => {println("value is null")
        "briup"
      }
    }
    //getOrElse 通过key取value，如果value存在则取出来
    //如果不存在则产生默认值，注意第二个参数是通过传名调用
    val v3 = m1.getOrElse(3,()=>"NULL")
    val v4 = m1.getOrElse(3,"NULL")

    //3 遍历
    for(e <- m1){
      println(e._1 +" " +e._2)
    }
    for((k,v)<- m1){
      println(k +" "+v)
    }
    for(k<- m1.keys){
      println(k + " "+m1(k))
    }
    m1.foreach(t => println(t._1+" "+t._2))

    //4 常用方法
    //+   ++  ++: (+= ++=) insert insertAll
    m3.+((4,"jack"))
    m3 + (5->"rose")
    println("-----------------------------")
    //5 拉链操作
    // zip内连接 zipAll zipWithIndex unzip  unzip3(相当于数据库中连接和查询操作）
    val a1=Array(1,2,3,4)
    val a2=Array("a","b","c")
    val r: Map[Int, String] =a1.zip(a2).toMap
    r.foreach(println)
    println("-----------------------------")

    val r1=a1.zipAll(a2,"A1","A2")
    r1.foreach(println)

    println("----------------------------")
    //unzip3
    val a3 = Array((1,"jack",20),(2,"rose",21),(3,"black",22))

    //select id,name,age from a3
    val (ids,names,ages)=a3.unzip3

    println(ids.getClass.getName)
    ids.foreach(println)

  }
}
