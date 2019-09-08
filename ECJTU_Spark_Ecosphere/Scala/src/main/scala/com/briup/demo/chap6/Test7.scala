package com.briup.demo.chap6

// 实现一个函数,检查某张牌的花色是否为红色
object Test7 extends Enumeration with App {
  val M = Value("♣")
  val T = Value("♠")
  val H = Value("♥")
  val F = Value("♦")

  def color(c: Test7.Value) {
    if (c == Test7.M || c == Test7.T) print("Black")
    else print("Red")
  }

  color(Test7.H)

}
