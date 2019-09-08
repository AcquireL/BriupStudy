package com.briup.sql

import java.time.YearMonth

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.Test
/*
  1.tbDate
  2003-1-1,200301,2003,1,1,3,1,1,1,1
	定义了日期的分类，将每天分别赋予所属的月份、星期、季度等属性，字段分别为 日期、年月、年、月、日、周几、第几周、季度、旬、半月;
	2.tbStock
	BYSL00000893,ZHAO,2007-8-23
	定义了订单表头，字段分别为订单号、交易位置、交易日期;
	3.tbStockDetail
	BYSL00000893,0,FS527258160501,-1,268,-268
	文件定义了订单明细，该表和 tbStock 以订单号进行关联，字段分别为订单号、行号、货品、数量、单价、金额;
 */
case class Date(date:String,yearMonth:String,year:String,month:String,day:String,week:String,weekth:String,season:String,ten_day:String,half_month:String)
case class Stock(order_id:String,patition:String,date:String)
case class StockDetaili(order_id:String,row:String,goods:String,number:Long,per:String,amount:Double)
class Order {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark=SparkSession.builder().master("local[*]").appName("order").getOrCreate()
  val sc=spark.sparkContext
  import spark.implicits._
  val rdd: RDD[String] =sc.textFile("hdfs://master:9000/spark/sql/order/tbDate.txt")
  val date=rdd.map(line=>{
    val field=line.split(",")
    Date(field(0),field(1),field(2),field(3),field(4),field(5),field(6),field(7),field(8),field(9))
  }).toDS()
  val rdd1=sc.textFile("hdfs://master:9000/spark/sql/order/tbStock.txt")
  val stock=rdd1.map(line=>{
    val Array(order_id,patition,date)=line.split(",")
    Stock(order_id,patition,date)
  }).toDS()
  val rdd2=sc.textFile("hdfs://master:9000/spark/sql/order/tbStockDetail.txt")
  val stockDetail=rdd2.map(line=>{
    val field=line.split(",")
    StockDetaili(field(0),field(1),field(2),field(3).trim.toLong,field(4),field(5).trim.toDouble)
  }).toDS()

  //1.计算所有订单每年的总金额
  @Test
  def fun1()={
    date.createTempView("tbDate")
    stock.createTempView("tbStock")
    stockDetail.createTempView("tbStockDetail")
    spark.sql("select a.order_id,a.amount,b.date from tbStockDetail a,tbStock b where a.order_id=b.order_id").createTempView("t1")
    spark.sql("select b.order_id,a.year,b.amount " +
      "from tbDate a left join t1 b on a.date=b.date").createTempView("t2")
    spark.sql("select order_id,year ,sum(amount) " +
      "over(partition by year,order_id order by year) amount " +
      "from t2").show()
  }

//  2.计算所有订单每年最大金额订单的销售额
//
//  3.所有订单中季度销售额前10位
//
//  4.列出销售金额在100000以上的单据
//
//  5.所有订单中每年最畅销货品
}
