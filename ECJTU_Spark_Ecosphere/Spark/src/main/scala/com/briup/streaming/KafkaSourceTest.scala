package com.briup.streaming

import com.briup.streaming.JsonFileSourceTest.{df, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSourceTest extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark=SparkSession.builder().master("local[*]").appName("KafkaSourceTest").getOrCreate()
  import spark.implicits._

  val df=spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers","192.168.29.133:9092")
    .option("subscribe","shopInfos").load()

  val res: DataFrame = df.selectExpr("CAST(value AS STRING)")

  val sq=res.writeStream.format("console")
    .option("truncate",false)
    .start()
  sq.awaitTermination()
  spark.close()
}
