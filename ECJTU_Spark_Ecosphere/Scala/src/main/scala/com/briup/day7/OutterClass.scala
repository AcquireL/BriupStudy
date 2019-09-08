package com.briup.day7

class OutterClass {
  //为属性起别名
  point => val value:String="3.1415"
  class InnerClass{
    val e:String  = "0.618"
    def test()={
      println(OutterClass.this.value)
      println(point.value)
    }
  }

}
object Test{
  def main(args: Array[String]): Unit = {
    //1 不同的外部类对象所访问的内部类时不同的类
    val one = new OutterClass
    val two = new OutterClass



  }
}