package com.briup

import org.apache.spark.{SparkConf, SparkContext}

package object core {
  def getSC(name:String,master:String="local[*]"): SparkContext ={
    SparkContext.getOrCreate(new SparkConf().setAppName(name).setMaster(master))
  }
}
