package com.briup.streaming

import java.sql
import java.sql.{DriverManager, Timestamp}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, ForeachWriter}
import org.junit.Test

class BlackListMain {

  /*
        1.过滤掉位于黑名单文件中的用户信息
   */
  @Test
  def blackListFile() = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = getSpark("过滤黑名单操作")
    import spark.implicits._
    val ds = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .option("includeTimestamp", true)
      .load().as[(String, Timestamp)]

    val userInfoStream = ds.map {
      case (line, _) =>
        val Array(id, name) = line.split(" ")
        (id.trim.toInt, name)
    }.toDF("id", "name")

    //读取黑名单数据blackListFile.txt
    val blackListInfo = spark.read.text("files/blackListFile.txt")
      .toDF("blackName")

    //过滤黑名单中的数据
    val result1 = userInfoStream.join(blackListInfo, $"name" === $"blackName", "left")
      .filter($"blackName".isNull)
      .select($"id", $"name")

    //输出外部系统
    val query1 = result1.writeStream
      .format("console")
      .start()
    query1.awaitTermination()
    spark.close()
  }
  /*
        2.过滤掉位于黑名单表中的用户信息
           计算1minutes内访问累计达到20次用户，
           被标记为黑名单用户，将该用户存储到黑名单表中。
   */
  @Test
  def blackListTable() = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = getSpark("过滤黑名单操作2")
    import spark.implicits._
    val ds = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .option("includeTimestamp", true)
      .load().as[(String, Timestamp)]
    val userInfoStream = ds.map {
      case (line, times) =>
        val Array(id, name) = line.split(" ")
        (id.trim.toInt, name,times)
    }.toDF("id", "name","times")
    //读取黑名单数据
    val blackListTable=spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/briup")
      .option("user","root")
      .option("password","713181")
      .option("dbtable","blackList")
      .load()
      .select($"blackName")

    //1.过滤黑名单中的数据
    val result1 = userInfoStream.join(blackListTable, $"name" === $"blackName", "left")
      .filter($"blackName".isNull)
      .select($"id", $"name",$"times")

    //2.计算每个用户lminutes之间访问次数
    import org.apache.spark.sql.functions._
    val result2=result1.withWatermark("times","60 seconds")
      .groupBy(window($"times","1 minutes"),$"name",$"id")
      .count()
      .filter($"count">=10)
      .select($"id",$"name")
      .as[(Int,String)]

    //将结果输出到数据库表中
    val fw: ForeachWriter[(Int, String)] =new ForeachWriter[(Int, String)] {

      var conn:sql.Connection=null;
      var prestmt:sql.PreparedStatement=null;
      val url="jdbc:mysql://localhost:3306/briup?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE&useSSL=false"
      val user="root";
      val password="713181";

      override def open(partitionId: Long, epochId: Long): Boolean = {
        conn=DriverManager.getConnection(url,user,password)
        val sql="insert into blackList(blackId,blackName) values(?,?)";
        prestmt=conn.prepareStatement(sql);
        true
      }

      override def process(value: (Int, String)): Unit = {
        prestmt.setInt(1,value._1)
        prestmt.setString(2,value._2)
        prestmt.execute();
      }

      override def close(errorOrNull: Throwable): Unit = {
        if(prestmt!=null) prestmt.close()
        if(conn!=null) conn.close()
      }
    }
    val query2=result2.writeStream
      .foreach(fw)
      .outputMode(OutputMode.Complete())
      .start();
    /*val query1=result2.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()*/
    query2.awaitTermination()
    spark.close()

  }

}
