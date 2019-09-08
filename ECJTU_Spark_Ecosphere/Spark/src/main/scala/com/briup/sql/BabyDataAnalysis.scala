package com.briup.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.Test

//hdfs://172.16.0.4:9000/data/spark/baby/yob*
//1、130年中每年出生婴儿的男女比例
//2、出生婴儿男女比例最失调的N个年份
//3、名字叫做John,Harry,Mary,Marilyn婴儿的出生趋势
case class BabyYobInfo(name:String,gender:String,number:Long,year:String)
class BabyDataAnalysis {
  Logger.getLogger("org").setLevel(Level.WARN)
  val spark=SparkSession.builder().master("local[*]").appName("baby").getOrCreate()
  val sc=spark.sparkContext
  import spark.implicits._

  val rdd=sc.wholeTextFiles("hdfs://172.16.0.4:9000/data/spark/baby/yob*")
  //val rdd=sc.wholeTextFiles("hdfs://master:9000/spark/baby/yob*")
  val babyds: Dataset[BabyYobInfo] =rdd.flatMap{
    case (fileName,fileCnt)=>{
      val year=fileName.substring(fileName.lastIndexOf("/")).substring(4,8)
      val arr=fileCnt.split("\n").map(line=>{
        val Array(name,gender,number)=line.split(",")
        BabyYobInfo(name,gender,number.trim.toLong,year)
      })
      arr
    }
  }.toDS()
  //1、130年中每年出生婴儿的男女比例
  @Test
  def fun1()={
    //api
    /*babyds.groupBy("year").pivot("gender").sum("number")
      .selectExpr("year","M/F").toDF("year","ratio").show()*/
    babyds.groupBy("year").pivot("gender").sum("number")
      .selectExpr("year","M/F").toDF("year","ratio").createTempView("table")
    spark.sql("select * from table").show()
    //------------------
    //sql
    spark.close()
  }
  //2、出生婴儿男女比例最失调的N个年份 N=10
  @Test
  def fun2()={
    babyds.groupBy("year").pivot("gender").sum("number")
      .selectExpr("year","M/F").toDF("year","ratio").createTempView("ratioInfo")
    spark.sql("select year,abs(1-ratio) ratio from ratioInfo order by ratio limit 20").show()
    //spark.sql("select year from ratioInfo").show()
    spark.close()
  }
  //3、名字叫做John,Harry,Mary,Marilyn婴儿的出生趋势
  //按年分组，统计每年内 叫这几个名字的婴儿出生个数
  @Test
  def fun3()={
    val s=Seq("John","Harry","Mary","Marilyn")
    //babyds.filter(baby=>s.contains(baby.name)).groupBy("year","name").sum("number").show()
    babyds.createTempView("babyInfo")
    spark.sql("select year,name,sum(number) from babyInfo where name in ('John','Harry','Mary','Marilyn') group by year,name").show()
    spark.close()
  }
  //4、按性别计算每年最常用的1000个名字人数占当年总出生人数的比例（名字多样化分析）
  @Test
  def fun4()={
    babyds.createTempView("babyInfo")
    //每年出生人数的总数
    spark.sql("select year,sum(number) sum from babyInfo group by year").createTempView("t1")
    //spark.sql("select * from(select year,gender,name,number,row_number() over(partition by year,gender order by number desc) row from babyInfo) where row<1001").show()
    spark.sql("select year,gender,sum(number) sum1000 from (select * from(select year,gender,name,number,row_number() over(partition by year,gender order by number desc) row from babyInfo) where row<1001) group by year,gender").createTempView("t2")
    spark.sql("select a.year,b.gender,b.sum1000/a.sum ratio from t1 a left join t2 b on a.year=b.year order by a.year,b.gender").show(100)
    spark.close()
  }

  //5、按性别假定每年有频率最高的前N个名字(N最小)的婴儿人数总和超过当年该性别总人数的50%，
  // N值随年份的变化趋势。（名字多样化分析）
  //
  @Test
  def fun5()={
//       babyds.createTempView("babyInfo")
    //    spark.sql("select year,gender,sum(number)/2 sum from babyInfo group by year,gender order by year ").createTempView("t1")
    //    spark.sql("select year,name,gender,sum(number) over(partition by year,gender order by number desc) add_number,row_number() over(partition by year,gender order by number desc) row_number from babyInfo order by year,row_number").createTempView("t2")
    //    spark.sql("select * from (select b.year,b.gender,first_value(b.row_number) over(partition by b.year,b.gender order by b.year) N from t2 b left join t1 a on a.year=b.year and a.gender=b.gender where b.add_number>a.sum ) group by year,gender,N order by year,gender").show(100)
    babyds.createTempView("babyinfo")
    spark.sql("select year,gender,top,row,ratio from (select *,row_number() over(partition by year,gender order by number desc) top from (select *,sum1/sum2 ratio from (select *,row_number() over(partition by year,gender order by number desc) row,sum(number) over(partition by year,gender order by number desc) sum1,sum(number) over(partition by year,gender) sum2 from babyinfo) where sum1/sum2>=0.5 order by ratio) ) where top=1").show()

    spark.close()
  }
  //6、各年出生的男孩名字以d/n/y结尾的人数占当年的比例
  @Test
  def fun6()={
    babyds.createTempView("babyinfo")
    spark.sql("select year,gender,substring(name,length(name),1) name,sum(number) sum from babyinfo where substring(name,length(name),1) in('d','n','y') and gender='M' group by year,gender,substring(name,length(name),1)").createTempView("t1")
    spark.sql("select year,sum(number) sum from babyinfo group by year").createTempView("t2")
    spark.sql("select a.year,a.gender,a.sum/b.sum ratio from t1 a left join t2 b on a.year=b.year order by year").show()
  }
  //7、130年中，变化趋势最明显的前n个名字(可以考虑使用标准差度量)
  @Test
  def fun7()={
      babyds.createTempView("baseinfo")
    spark.sql("select gender,name,stddev(number) stddev from baseinfo group by gender,name").show()
  }
}
