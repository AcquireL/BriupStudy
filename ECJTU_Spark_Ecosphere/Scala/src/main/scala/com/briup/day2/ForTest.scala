package com.briup.day2

object ForTest extends App{
  //发生器 变量 <- 集合
  for( i <- 1 to 5){
    println("*" * i);
  }
  println(1.to(4));
  println("---------------------");
  for( i <- 1.to(10,2)){
    println("*" * i);
  }
  println("--------------------- ");
  //守卫
  for(i <- 1 to 10 if(i%2==0)){
    print(i + " ");
  }
  println("----------------------");
  //嵌套循环
  for(i <- 1 to 5;j <- 10.to(50,10)){
    println(i + " "+ j);
  }
  //函数
  //1. 函数和方法的区别
  // i 函数可以用变量接收
  // ii 方法就是函数
  //2. 声明函数
  //  i lambda   ()=>{}
  def fun(a:Int,b:Int): Unit ={
    a+b;
  }
  val fun1: (Int, Int) => Int = (a:Int, b:Int)=>a+b

  println("--------------------------")
  class FunTest extends Function2[Int,Int,Int]{
    override def apply(a:Int,b:Int)=a+b
  }
  val fun2=new FunTest;
  println(fun2(6,4))

  class FunTest2 extends Function2[Int,Int,Int]{
    override def apply(a:Int,b:Int)=a+b
  }



}
