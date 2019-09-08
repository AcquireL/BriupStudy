package com.briup.core

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.KeyValueTextInputFormat
import org.apache.spark.rdd.RDD
import org.junit.Test

class TopicRDDTest {
  @Test
  def topic()={
    val sc = getSC("topic")
    val userinfo =sc.textFile("hdfs://172.16.0.4:9000/data/spark/topic/userInfo").map(line=>{
      val user_orderid: Array[String] =line.split(",")
      (user_orderid(0),user_orderid(1))
    })
    val linkinfo=sc.textFile("hdfs://172.16.0.4:9000/data/spark/topic/linkInfo").map(line=>{
      val user_seeid: Array[String] =line.split(",")
      (user_seeid(0),user_seeid(1))
    })
    val topicinfo=sc.textFile("hdfs://172.16.0.4:9000/data/spark/topic/topicInfo").map(line=>{
      val topicid_name: Array[String] =line.split(",")
      (topicid_name(0),topicid_name(1))

    })
    linkinfo.subtract(userinfo).map(x=>(x._2,x._1)).join(topicinfo).foreach(println)
    //topicinfo.foreach(println)
    //linkinfo.foreach(println)
  }
}
