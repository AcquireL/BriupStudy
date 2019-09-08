package com.briup.day3

object ArrayTest extends App {
  //1.创建
  //String[] a1=new String[5];
  val a1= new Array[String](5);
  //对象的统一构建原则 伴生对象apply
  val a2 = Array.apply("Hello","world","briup")
  val a3= Array[String]("Hello","world","briup")
  //空数组
  val a4 =Array.empty

  //把数组中的值直接和某个变量对应起来
  val Array(a,b,c) = Array.apply("Hello","world","briup")
  //2.元素的访问 下标
  println("a2 第一个值："+a2(0))
  println("a2 第二个值："+a2.apply(1))

  println("a5 中的值："+a+" "+b+" "+c)
  //3.遍历
  println("-------------------------")
  for(i <- a3){
    println(i)
  }
  for(i<- 0 to a3.length-1){
    println(a3(i))
  }
  println("------------------------")
  //a3.foreach((i)=>{println(i)})
  a3.foreach(println _)
  //4.类中的方法

  // ++ ++: +: addString clone toBuffer
  println("------------------------")
  val a6:Array[String]=a2.++(a3)
  a6.foreach(println)
  println("---------------------")
  val a7 = a2.+:("jack")
  a7.foreach(println)
  println("--------------------")
  val a8 = a2 +: "jack"
  a8.foreach(println)
  println("--------------------")
  val sb=new StringBuilder
  a2.addString(sb)
  println(sb.toString())
  println("----------------------")


}
