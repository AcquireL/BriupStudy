package com.briup.core.homework


object test extends App {
  def gcd(x: Int, y: Int): Int = {
    var temp = x % y
    if (temp == 0) {
      y
    } else {
      gcd(y, temp)
    }
  }
  println(gcd(2,4))
}