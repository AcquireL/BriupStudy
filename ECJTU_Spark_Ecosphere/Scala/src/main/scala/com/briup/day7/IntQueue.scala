package com.briup.day7

import scala.collection.mutable.ArrayBuffer

abstract class IntQueue {
  def get(): Int;

  def put(x: Int);
}

class BasicIntQueue extends IntQueue {
  private val buf = new ArrayBuffer[Int]

  //永远取第一个数据
  def get() = buf.remove(0)

  //永远插入到最后
  def put(x: Int) {
    buf += x
  }
}

trait Incrementing extends IntQueue {
  abstract override def put(x: Int) {
    super.put(x + 1)
  }
}

trait Doubling extends IntQueue {
  abstract override def put(x: Int) {
    super.put(2 * x)
  }
}

object TestClient extends App {
  val queue1 = (new BasicIntQueue with Incrementing with Doubling)
  queue1.put(2) //Doubling.put(2*2)->Incrementing.put(4+1)
  println(queue1.get()) //result is 5
  val queue2 = (new BasicIntQueue with Doubling with Incrementing)
  queue2.put(2) //Incrementing.put(2+1)->Doubling.put(2*3)
  println(queue2.get()) //result is 6
}
