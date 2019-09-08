package com.briup.day3

object TryTest {
  def main(args: Array[String]): Unit = {
    show("hello")
    val str:String=null;
    try{
      show(str)
    }catch {
      case e:NullPointerException=>{
        e.printStackTrace()
      }
      case e:Exception=>{
        e.printStackTrace()
      }
      case _ =>{}
    }
    println("---------------------------")
  }
  def show(msg:String)={
    if(msg == null){
      throw new NullPointerException("msg is null")
    }else{
        println("msg:" +msg)
    }
  }

}
