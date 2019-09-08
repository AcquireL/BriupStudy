package com.briup

import org.apache.spark.sql.SparkSession

package object streaming {
  def getSpark(name: String, master: String = "local[*]"): SparkSession = {
    SparkSession.builder().appName(name).master(master).getOrCreate()
  }
}
