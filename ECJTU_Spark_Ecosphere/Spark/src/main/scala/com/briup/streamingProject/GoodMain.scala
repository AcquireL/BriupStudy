package com.briup.streamingProject

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
/*
    定义最终输出类型（实时商品关注度，5min内的平均关注度，商品ID，时间）
 */
case class ResultGoodData(nowData:Double,avgData:Double,goodID:String,times:Timestamp)
//原始数据（商品ID，时间，关注度）
//状态类（5min内的平均关注度，标记属性 isFlag(true,没有过时))
case class ResultGoodState(nowData:Double,avgData:Double,goodID:String,times:Timestamp,isFlag:Boolean)
object GoodMain extends App {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark = SparkSession.builder()
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation","Goods-checkPonit")
    .appName("商品关注度").getOrCreate()
  import spark.implicits._
  //1.从Kafka的shopInfos读取数据

  val kafkaStream=spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.29.133:9092")
      .option("subscribe","shopInfos")
      .load()
      .selectExpr("cast(value as string)")
      .as[String]
  //2.整理数据格式
  val formatStream=kafkaStream.map(line=>{
    val Array(goodID,browerNum,stayTime,isColl,buyNum,times)=line.split("::")
    //计算商品的关注度
    val rank=browerNum.trim.toDouble*0.8+stayTime.trim.toDouble*0.6+isColl.trim.toDouble*1+buyNum.trim.toDouble*1
    (goodID,rank,new Timestamp(times.trim.toLong))
  }).toDF("goodID","rank","times")
    .as[(String,Double,Timestamp)]
  //3.计算每个商品的实时关注度曲线以及连续5分钟不间断的平均关注度曲线，
  val fun1=(key:String,iter:Iterator[(String,Double,Timestamp)],state:GroupState[ResultGoodState])=>{
    val list=iter.toList
    //1.判断有没有过时
    if( state.hasTimedOut){
      //过时
      state.remove()
      //返回ResultGoodData对象
      ResultGoodData(0.0,0.0,key,null)
    }else{
      //2.判断状态存不存在
      if(state.exists){
        //存在 代表此数据已经出现过
        val oldData=state.get
        //list 存储的上一次的数据信息
        //当前关注度
        val nowData=list.map(_._2).sum
        //5min中的关注度
        val avgData=oldData.avgData+nowData
        val minTimes=oldData.times
        //构建新的状态对象
        val newState=ResultGoodState(nowData,avgData,key,minTimes,false)
        state.update(newState)
        //返回ResultGoodData对象
        ResultGoodData(newState.nowData,newState.avgData,key,newState.times)
      }else{
        //不存在 第一次出现
        //构建状态对象
        //获取该商品数据第一次出现的最早时间
        val minTimes=list.minBy(_._3.toString)._3
        //当前商品的关注度
        val nowData=list.map(_._2).sum
        //5min内商品的关注度
        val avgData=nowData
        val resultState=ResultGoodState(nowData,avgData,key,minTimes,false)

        //更新状态
        state.update(resultState)
        //设置过时时间
        state.setTimeoutDuration("5 minutes")
        //返回ResultState对象
        ResultGoodData(resultState.nowData,resultState.avgData,key,resultState.times)
      }
    }
  }
  val res=formatStream.groupByKey(_._1)
    .mapGroupsWithState[ResultGoodState,ResultGoodData](GroupStateTimeout.ProcessingTimeTimeout())(fun1)
    //.flatMapGroupsWithState[ResultGoodState,ResultGoodData](OutputMode.Append(),GroupStateTimeout.ProcessingTimeTimeout())(fun1)


  //输出控制台
/*  val query1=res.writeStream
    .format("console")
    .outputMode(OutputMode.Update())
    .option("truncate", false)
    .start()*/

  //输出到Kafka
  val query1=res.selectExpr("cast(concat('{\"window\":\"',cast(times as String),'\",\"key\":\"',goodID,'\"}') as String) as key","cast(concat('{\"window\":\"',cast(times as string),'\",\"key\":\"',cast(goodID as String),'\",\"avg\":\"',cast(avgData as String),'\",\"now\":\"',cast(nowData as String),'\"}') as String) as value")
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers","192.168.29.133:9092")
  .option("topic","lwj")
  .outputMode(OutputMode.Update())
  .start();
  query1.awaitTermination()
  spark.close()
}
