package com.briup.day7

class Creature {
  val range : Int=10
  //val env: Array[Int] = new Array[Int] ( range)
  //1 延迟加载
  lazy val env: Array[Int] = new Array[Int] ( range)
}
//2 提前定义{ 提前定义块 需要提前初始化的字段}
class Ant extends {
  override val range=2
} with Creature{
}
/*class Ant extends Creature {
  override val range=2
}*/

object CreatureTest extends App{
  //1 超类对象分配空间，初始化默认值给超类属性，调用超类的构造方法
  //2 子类对象分配空间，初始化默认值给子类属性，调用子类的构造方法
  val a = new Ant
  //请问此处数组多长？？？
  println(a.env.length)
}
