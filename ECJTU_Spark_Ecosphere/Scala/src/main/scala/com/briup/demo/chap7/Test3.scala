package com.briup.demo.chap7

/*
      编写一个包random,加入函数nextInt():Int,nextDouble():Double,setSeed(seed:Int):Unit。
      生成随机数的算法采用线性同余生成器:
          后值 = (前值 * a + b)mod 2^n
          其中,a = 1664525,b=1013904223,n = 32,前值的初始值为seed
 */
package random {

  package object random {
    var seed: Int = _
    val a = BigDecimal(1664525)
    val b = BigDecimal(1013904223)
    val n = 31

    def nextInt(): Int = {
      val temp = (seed * a + b) % BigDecimal(2).pow(n)
      seed = temp.toInt
      seed
    }

    def nextDouble(): Double = {
      val temp = (seed * a + b) % BigDecimal(2).pow(n)
      seed = temp.toInt
      temp.toDouble
    }


  }

}

package Test3 {

  import random.random

  object Test3 extends App {
    random.seed = 4
    println(random.nextDouble())
    println(random.nextDouble())
    println(random.nextDouble())
    println(random.nextDouble())
    println(random.nextInt())
    println(random.nextInt())
    println(random.nextInt())
    println(random.nextInt())

  }

}

