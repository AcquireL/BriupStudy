package com.briup.day9

import com.briup.demo.chap5.Student

abstract class Person (n:String){
  var name:String;

}
class Studetn(override var name:String) extends Person(name){

}
class ClassUtil[-T <:Person]{
  //点名
 /* def showName[U >: T](stus:Array[U]){
      stus.foreach(println)
  }*/
}
object ClassTest{
  def main(args: Array[String]): Unit = {
    val cu=new ClassUtil[Studetn]
    val stus1=Array[Person](
      new Studetn("jack"),
      new Studetn("bob"),
      new Studetn("rose")
    )
  /*  val stus2=Array[Student](
      new Studetn("jack"),
      new Studetn("bob"),
      new Studetn("rose")
    )
  */
  }
}