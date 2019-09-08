package com.briup.day3

//访问控制， 包下资源可见性
class Person {
  //private[可见性范围]  protected 默认（public）
  //private 可以使用[包名/类名]限定访问范围
  //在范围内开放，在范围外关闭

  //private 访问范围[this]
  //仅有自己能用，有元类或对象不能访问
  var name:String = _;
  var age:Int = _;

  def this(name:String,age:Int)={
    this();
    this.name=name;
    this.age=age;
  }
}

