package com.briup.demo.chap6
/*
    编写一个Scala应用程序,使用App特质,以反序打印命令行参数,用空格隔开。
    举例来说,scala Reverse Hello World应该打印World Hello  在终端上实现
 */
object Test5 extends App {
  args.reverse.foreach( args => print(args+ " "))
}
