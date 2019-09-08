package com.briup.day9

class Util[T <% Comparable[T]] {
  def smaller(a: T, b: T): T = {
    if (a.compareTo(b) > 0)
      b;
    else
      a;
  }
}

object UtilObject extends App {
  val u = new Util[String]
  println(u.smaller("briup", "hello"))
  //Int 和 Comparable 没关系

  //Int 隐式转化RichInt - > Comparable
  Predef
  val u1 = new Util[Int]
  println(u1.smaller(1, 2))
}