package com.briup.day6

class StreamTest {

  // n :: n+1 :: n+2 ....... :: Nil
 /* def numsFrom(n:Long): List[Long] ={
    n :: numsFrom(n+1)
  }*/
  def numsFrom(n:Long):Stream[Long]={
    n #:: numsFrom(n+1)

  }


  //stream的构建
  //apply
  //构造器
  //Stream.from
}
