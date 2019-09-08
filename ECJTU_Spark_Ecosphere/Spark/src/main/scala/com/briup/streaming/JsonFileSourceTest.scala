package com.briup.streaming

import com.briup.streaming.TextFileSourceTest.{df, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object JsonFileSourceTest extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark=SparkSession.builder().master("local[*]").appName("TextFileSourceTest").config("spark.sql.streaming.schemaInference",true).getOrCreate()
  import spark.implicits._
  val s=new StructType().add("properties",
      new StructType().add("clientIP","string")
      .add("clientPort","int")
      .add("httpMethod","string"))
  //自定义json结构
  //val df =spark.readStream.format("json").option("multiline",true).schema(s).load("Spark/files/jsonfile")



  //使用.config("spark.sql.streaming.schemaInference",true)自动读取json结构
  val df =spark.readStream.format("json").option("multiline",true).load("Spark/files/jsonfile")
  df.printSchema()
  val sq=df.writeStream.format("console")
    .option("truncate",true)
    .start()

  sq.awaitTermination()
  spark.close()

}
