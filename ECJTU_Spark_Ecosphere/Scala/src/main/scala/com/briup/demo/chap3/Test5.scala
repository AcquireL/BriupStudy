package com.briup.demo.chap3
/*
    如何计算Array[Double]的平均值？
 */
object Test5 extends App {
  val arr=Array[Double](1,2,3,4)
  def arrayAvg(arr:Array[Double]):Double={
    var sum:Double=0;
    for(i<-arr){
      sum+=i;
    }
    sum/arr.length
  }
  println(arrayAvg(arr))
}
