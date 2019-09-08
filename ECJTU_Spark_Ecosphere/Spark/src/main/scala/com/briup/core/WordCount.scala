package com.briup.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //1 SparkConf
    // spark-submit master name 在命令行里设置
    val conf= new SparkConf().setMaster("local[*]").setAppName("lwj_wordcount")
    //2 SparkContext
    val sc= new SparkContext(conf);
    //3 RDD
    val lines = sc.textFile("file:///D:\\spark-2.4.3-bin-hadoop2.7\\README.md")
    //4 执行操作 flatten
    //    Mapper     -------->   shuffle  ------->   Reducer
    val rdd1=lines.flatMap(_.split(" ")).groupBy(x=>x).mapValues( x=>x.size);
    rdd1.foreach(println)
    //* 序列化执行结果
    rdd1.saveAsTextFile("file:///D:\\idea-hadoop-workpace\\ECJTU_Spark_Ecosphere\\Spark\\src\\main\\scala\\com\\briup\\core\\wc_result")
    //5 关闭sc
    sc.stop();
  }

}
