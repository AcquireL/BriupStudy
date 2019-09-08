package com.briup.demo.chap6
/*
     编写一个扑克牌4种花色的枚举,让其toString方法分别返回♣,♦,♥,♠
 */
object Test6 extends Enumeration with App {
  val M = Value("♣")
  val T = Value("♠")
  val H = Value("♥")
  val F = Value("♦")
  override def toString(): String = {println(s"$M $T $H $F"); " "}

  Test6.toString()

}
