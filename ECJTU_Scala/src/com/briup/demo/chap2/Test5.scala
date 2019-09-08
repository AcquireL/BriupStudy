package com.briup.demo.chap2

object Test5 {
  def main(args: Array[String]): Unit = {
    countDown(10)
  }
  //编写一个过程打印n到0
  def countDown(num:Int){
    for(i<- 0 to num reverse)
      println(i)
//    0 to num  reverse foreach print
  }

}
