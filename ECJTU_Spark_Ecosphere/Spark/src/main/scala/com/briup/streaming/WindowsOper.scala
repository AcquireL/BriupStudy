package com.briup.streaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming.FileStreamSource
import org.apache.spark.sql.streaming.OutputMode
import org.junit.Test

class WindowsOper {

  @Test
  def windows1()={
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark=getSpark("窗口操作")
    import spark.implicits._
    val ds=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .option("includeTimestamp",true)
      .load().as[(String,Timestamp)]

    val wordStream: Dataset[(String, Timestamp)] =ds.flatMap{
      case(line,times)=>
        val arr=line.split(" ");
        arr.map(word=>(word,times))
    }.toDF("word","timestamp").as[(String,Timestamp)]

    val result1=wordStream.groupBy($"word").count()

    import org.apache.spark.sql.functions.window
    val result2=wordStream
      .groupBy(window($"timestamp","5 seconds"),$"word").count()

    val query1=result2.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .option("truncate",false)
      //.trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    query1.awaitTermination()
    spark.close()

  }
}
