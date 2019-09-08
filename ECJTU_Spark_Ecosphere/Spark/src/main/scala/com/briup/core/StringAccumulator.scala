package com.briup.core

import org.apache.spark.util.AccumulatorV2

class StringAccumulator extends AccumulatorV2[String,String]{
  //代表初始值
  var str=""
  override def isZero: Boolean = if(str.length==0) true else false
  override def copy(): AccumulatorV2[String, String] = {
    val a= new StringAccumulator
    a.str=this.str
    a
  }
  override def reset(): Unit = str=""
  override def add(v: String): Unit = str+=v
  override def merge(other: AccumulatorV2[String, String]): Unit ={
    /*case x:StringAccumulator =>str+=x.str
    case _ => throw  new ClassCastException("需要 StringAccumulator 类型")*/
  }
  override def value: String = str
}
