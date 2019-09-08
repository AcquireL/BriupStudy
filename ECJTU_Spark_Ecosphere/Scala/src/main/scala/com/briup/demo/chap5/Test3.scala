package com.briup.demo.chap5
/*
     编写一个Time类，加入只读属性hours和minutes，
     和一个检查某一时刻是否早于另一时刻的方法before(other:Time):Boolean。
     Time对象应该以new Time(hrs,min)方式构建。其中hrs以军用时间格式呈现(介于0和23之间)
 */
class Test3(val hours:Int,val minutes:Int) {
  def before(other:Test3)={
    hours < other.hours || (hours == other.hours && minutes < other.minutes)
  }
  override def toString() ={ println( hours +":" +minutes); hours +":" +minutes}

}
object Test3 extends App {
  val time:Test3 = new Test3(8,45)
  val time1:Test3 = new Test3(6,34)
  time.toString()
  println(time.before(time1))
}