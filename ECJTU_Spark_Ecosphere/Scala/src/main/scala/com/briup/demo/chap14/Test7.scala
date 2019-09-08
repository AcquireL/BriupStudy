package com.briup.demo.chap14

object Test7 extends App {
  val price=List(1,2,3)
  val qualtities=List(0.1,0.2,0.3)

  println((price zip qualtities) map (Function.tupled(_*_)))

  println(price zip qualtities map (p=>p._1*p._2) )

}
