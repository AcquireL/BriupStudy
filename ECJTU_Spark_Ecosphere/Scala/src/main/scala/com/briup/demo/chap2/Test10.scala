package com.briup.demo.chap2
/*
      编写函数计算xn,其中n是整数，使用如下的递归定义:
      •	xn=y2,如果n是正偶数的话，这里的y=x(n/2)
      •	xn = x*x(n-1),如果n是正奇数的话
      •	x0 = 1
      •	xn = 1/x(-n),如果n是负数的话
      不得使用return语句
 */
object Test10 {
  def main(args: Array[String]): Unit = {
      println(function(2,4))
  }
  def function(x:Double,n:Int):Double={
    if(n==0) 1
    else if(n>0 && n%2==0) function(x,n/2)*function(x,n/2)
    else if(n>0 && n%2!=0) x*function(x,n-1)
    else 1/function(x,-n)
  }

}
