package com.briup.core

import org.apache.spark.{SparkConf, SparkContext}

package object homework {
  def getSC(name:String,master:String="local[*]"): SparkContext ={
    SparkContext.getOrCreate(new SparkConf().setAppName(name).setMaster(master))
  }
}
