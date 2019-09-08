package com.briup.day7

import javax.swing.JFrame


trait LoggedException extends Exception
class HappyException extends LoggedException{} //编译正确，运行正确
class UnHappyException extends NullPointerException with LoggedException //编译正确，运行报错

//类中显示父类必须与超类存在子父类之间的关系。
//会报继承不合法
object ExceptionTest extends App {
  new UnHappyException
}
