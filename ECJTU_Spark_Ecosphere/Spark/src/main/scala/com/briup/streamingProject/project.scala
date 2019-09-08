package com.briup.streamingProject

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object project extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark = SparkSession.builder().master("local[*]").appName("KafkaSourceTest").getOrCreate()

  import spark.implicits._

  //读取数据
  val df: DataFrame = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "192.168.29.133:9092")
    .option("subscribe", "shopInfos").load()
  //.as[(String,Int,Float,Int,Int,Timestamp)]

  //数据处理，
  //mianhua::5::2.4902477:: 0:: 0::1566784714927
  //商品名称、浏览量次数、停留时间、是否收藏（-1为取消收藏）、购买次数、事件时间
  // case(good,browse_num,stay_time,collection,buy_num,time)=>{
  //      (good,browse_num,stay_time,collection,buy_num,time)
  //    }
  val res: DataFrame = df.selectExpr("CAST(value AS STRING)")


      val sq = res.writeStream.format("console")
        .option("truncate", false)
        .start()
      sq.awaitTermination()
      spark.close()

  }
