package com.briup.day4

class Student(var name:String,var age:Int,var score:Double) extends Ordered[Student]{

  override def compare(that: Student): Int =this.age-that.age

  override def toString:String = "Student " +name+" "+age+" "+score

}
