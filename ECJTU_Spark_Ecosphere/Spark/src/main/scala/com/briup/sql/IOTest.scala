package com.briup.sql

import org.apache.log4j.{Level, Logger}

object IOTest extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark = getSpark("Window")
  import spark.implicits._
  val s = Seq(Stu(1, "张三1", 1, 88.0D),
    Stu(4, "张三4", 2, 78.0D),
    Stu(3, "张三3", 1, 79.0D),
    Stu(8, "张三8", 1, 88.0D),
    Stu(5, "张三5", 2, 85.0D),
    Stu(2, "张三2", 2, 85.0D),
    Stu(6, "张三6", 1, 89.0D),
    Stu(7, "张三7", 2, 90.0D)).toDS
  s.createTempView("scores")

  //写 默认写出去的格式
  //s.write.mode("append").save("Spark/write1")
  //s.write.format("json").save("Spark/write2")
  //s.write.format("csv").option("sep",",").option("inferSchema","true").option("head","true").save("Spark/write3")

  //读
  spark.read.load("Spark/write1").show()
  //spark.read.format("json").load("Spark/write2").show()
  //spark.read.format("csv").option("sep",",").option("inferSchema","true").option("head","true").load("Spark/write3").show()

  //hdfs  mysql
  //s.write.saveAsTable("student")
  spark.close()

}
