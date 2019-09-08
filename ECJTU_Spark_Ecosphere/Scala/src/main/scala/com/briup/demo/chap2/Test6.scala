package com.briup.demo.chap2

/*
     编写一个for循环,计算字符串中所有字母的Unicode代码的乘积。
     举例来说，"Hello"中所有字符串的乘积为9415087488L
 */
object Test6 {
  def main(args: Array[String]): Unit = {
    function("Hello")
  }
  def function(str:String )={
    var sub:Long=1
    for(i<- str){
      sub=sub * i.toLong
    }
    print(sub)
  }
}
