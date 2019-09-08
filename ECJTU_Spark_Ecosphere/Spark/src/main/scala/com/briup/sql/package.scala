package com.briup

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

package object sql {
  def getSC(name: String, master: String = "local[*]"): SparkContext = {
    SparkContext.getOrCreate(new SparkConf().setAppName(name).setMaster(master))
  }

  //spark SparkSession
  def getSpark(name: String, master: String = "local[*]"): SparkSession = {
    SparkSession.builder().appName(name).master(master).getOrCreate()
  }

}
