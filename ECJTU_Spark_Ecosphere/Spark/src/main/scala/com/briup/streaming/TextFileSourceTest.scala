package com.briup.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TextFileSourceTest extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark=SparkSession.builder().master("local[*]").appName("TextFileSourceTest").getOrCreate()
  import spark.implicits._
  val df=spark.readStream.format("text").option("path","Spark/files/textfile").load()
  val sq=df.writeStream.format("console").option("truncate",true).start()
  sq.awaitTermination()
  spark.close()

}
