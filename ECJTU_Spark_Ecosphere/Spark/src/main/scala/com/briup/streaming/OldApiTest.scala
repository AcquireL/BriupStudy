package com.briup.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

object OldApiTest extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  //1 获取DStream
  val conf=new SparkConf().setMaster("local[*]").setAppName("OldApiTest")
  val ssc =new StreamingContext(conf,Duration.apply(1000))
  //Iterator[RDD[String]]
  val ds=ssc.socketTextStream("127.0.0.1",9999)
  //2
  val res=ds.filter(_!="").flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
  res.print()

  //3 开启实时处理任务
  ssc.start()
  ssc.awaitTermination()
  ssc.stop()
}
