package com.briup.day4

object ArrOfArrTest {
  def main(args: Array[String]): Unit = {
    //scala高维数组和java中的含义是一致的
    //数组中的元素还是数组
    //1 创建
    val arr1 =new Array[Array[Int]](5);
    val arr2=Array.ofDim[Int](3,4)
    val arr3=Array.ofDim[Int](3,4,5)
    //2 存取值
    //val i=arr2.apply(1).apply(2)
    val i1=arr2(1)(2)

  }
}
