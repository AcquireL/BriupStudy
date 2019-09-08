package com.briup.day5

class CollectionTest {
  def rangTest(): Unit ={
    val r= new Range(1,10,2)
    val r1=Range(1,10,2)
    val r2= 1 to 10 by 2
    val r3 = 1 until 10 by 2
    println(r(2))
    val r4=r.+:(100)
    println(r4.getClass.getName)
  }


}
