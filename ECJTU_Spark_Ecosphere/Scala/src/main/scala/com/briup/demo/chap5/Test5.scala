package com.briup.demo.chap5

import scala.beans.BeanProperty

/*
     创建一个Student类，加入可读写的JavaBeans属性name(类型为String)和id(类型为Long)。
     有哪些方法被生产？(用javap查看。)你可以在Scala中调用JavaBeans的getter和setter方法吗？
     应该这样做吗？

     name(),name_=(),id(),id_=(),setName(),getName(),setId(),getId()
 */
class Test5 {

}
class Student(var name:String,var id:Long){
  @BeanProperty var beanName:String = _
  @BeanProperty var beanId:Long = _
  def this(name:String,id:Long,age:Int){
    this(name,id)
    beanName=name
    beanId=id
  }
  name_=("Tom")


  override def toString: String = name +" " +id

  def print(): Unit ={
    setBeanName("BeanBob")
    setBeanId(16)
    println(getBeanName+"  "+getBeanId)
  }
}
object Student extends App {
  def apply(name:String,id:Long):Student={
    new Student(name,id)
  }
  val s:Student = Student("jack",18)
  val s1:Student = new Student("Tom" ,22)
  /*println(s.toString)
  s.print()*/
  s1.print()
}