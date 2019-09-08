package com.briup.day7

trait Trait1 {
  def log(file:String)
}
trait Trait2 extends Trait1{
   def log(file: String): Unit = {
    println("trait2")
  }
}
trait Trait3 extends Trait1{
   def log(file: String): Unit = {
    println("trait3")
  }
}
trait TimestampLogger extends Trait1{
  abstract override def log(msg:String){
    //使用了super调用父类方法时，必须用abstra overri 修饰该方法。
    super.log(new java.util.Date()+" "+msg )
  }
}