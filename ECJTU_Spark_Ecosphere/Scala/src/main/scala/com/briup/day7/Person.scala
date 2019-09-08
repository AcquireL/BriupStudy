package com.briup.day7

class Person {
  //类中属性 var => get/set  val => get
  //字段 Field var + get/set   val + get
  var name:String = _
  val age:Int = 0

}
object Test{
  def main(args: Array[String]): Unit = {
    val p=new Person
    p.name_=("jack")
    println(p.name)
    println(p.age)
  }
}