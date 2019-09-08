package com.briup.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDD {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("lwj_create")
    //conf.set("spark.name","local[*]")
    val sc=SparkContext.getOrCreate(conf)

    //----------从文件中拿，path可以写成具体文件，也可以指定到目录
    val rdd1: RDD[String] =sc.textFile("Spark/files/info.txt")
    val rdd2: RDD[(String, String)] =sc.wholeTextFiles("Spark/files/info.txt")

    //-----通过Seq构建  RDD---------
    val rdd3=sc.parallelize(1 to 10)
    val rdd4=sc.makeRDD(1 to 10)
    //-----通过hadoopFile Sequencefile
    //val rdd5=sc.newAPIHadoopFile("dfs://")
    println("----------------------------")
    rdd1.foreach(println)
    println("----------------------------")
    println("rdd1 rdd2 rdd3 rdd4 的id："+rdd1.id+" "+rdd2.id+" "+rdd3.id+" "+rdd4.id)
    println("---------------------------")
    println("rdd1 的分区列表")
    rdd1.partitions.foreach(println)
    println("---------------------------")
    println("rdd1 分区个数："+rdd1.getNumPartitions)
    println("rdd2 分区个数："+rdd2.getNumPartitions)
    println("rdd3 分区个数："+rdd3.getNumPartitions)
    println("rdd4 分区个数："+rdd4.getNumPartitions)

  }

}
