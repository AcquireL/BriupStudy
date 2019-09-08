package com.briup.demo.chap2

object Test1 {
  def main(args: Array[String]): Unit = {
    signum(1)
    signum(-1)
    signum(0)
  }
  //判断>0,返回1
  //<0，返回-1
  //=0，返回0
  def signum(num:Int){
    if(num>0) print(1)
    else if(num<0) print(-1)
    else print(0)
  }
}
