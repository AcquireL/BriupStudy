package com.briup.streaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SocketSourceTest extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark=SparkSession.builder().master("local[*]").appName("socketSouurce").getOrCreate()
  import spark.implicits._

  //事件时间 产生数据的时间
  val df =spark.readStream.format("socket").option("host","127.0.0.1").option("port",9999).option("includeTimestamp",true).load()


  //2 处理数据
  //使用窗口函数的条件
  //  1.必须要有聚合函数
  //  2.必须有时间类型的字段
  val w_t=df.as[(String,Timestamp)].flatMap{
    case(value,time)=>{
      val arr=value.split(" ")
      arr.map(word=>(word,time))
    }
  }
  import org.apache.spark.sql.functions._
  val res=w_t.groupBy(window($"timestamp","30 second"),$"word").count()

  //df -----> df.writeStream.start()
  //print("当前datafram是否是流式"+df.isStreaming)
   /*val w_c= df.flatMap(row=>{
     row.getAs[String]("value").split(" ").map(word=>(word,1))
   })
  val res: DataFrame =w_c.toDF("word","number").groupBy("word").sum("number")
*/
  //df.show()

  //3 开启任务  Append Complete Update
  val sq=res.writeStream
    .outputMode("complete")
    .option("truncate",true)
    .format("console").start()
  sq.awaitTermination()
  spark.close()
}
