package com.briup.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, types}
import org.junit.Test

class DFAndDSTest {
  //create DataFrame
  //1 读取外部数据
  @Test
  def cDF1() = {
    val spark = getSpark("c1")
    //引入spark对象中的隐式实体
    import spark.implicits._
    val df: DataFrame = spark.read.csv("files/topic/linkinfo")
    df.show()
    spark.close()
  }

  @Test
  def cDF2() = {
    val spark = getSpark("c2")
    import spark.implicits._
    val s: Seq[Person] = Seq(Person("jack", 20), Person("terry", 25))
    //元组   case class
    val df1: DataFrame = spark.createDataFrame[Person](s)
    val df2 = s.toDF("n", "a").show()
    df1.show()
  }

  @Test
  def cDF3() = {
    val spark = getSpark("c3")
    import spark.implicits._
    val sc = spark.sparkContext
    val s = Seq(Row("jack", 20), Row("terry", 40))
    val rdd = sc.parallelize(s)
    val s_f = Seq(StructField("name", StringType), StructField("age", IntegerType))
    val schema = types.StructType(s_f)
    val df3 = spark.createDataFrame(rdd, schema)
    df3.show()
    spark.close()
  }

  @Test
  def cDS() = {
    val spark = getSpark("cDS")
    import spark.implicits._
    val s = Seq(Person("jack", 20), Person("terry", 40))
    val ds = spark.createDataset[Person](s)
    //    ds.createTempView("person")
    ds.createGlobalTempView("person")
    spark.newSession().sql("select name,age from global_temp.person where age > 30").show()
    //ds.createGlobalTempView()
    //
    /*ds.createGlobalTempView()
    ds.createOrReplaceGlobalTempView()*/

    //val res=spark.sql("select name,age from person where age > 30").show()

    /* ds.show()
     s.toDS()
     s.toDF().as[Person]*/
    spark.close()
  }

  @Test
  def functionTest() = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = getSpark("functionTest")
    val sc = spark.sparkContext
    import spark.implicits._
    //-----------------
    val stus = Seq(Student(1001, "jack", "M", 20),
      Student(1002, "Tom", "M", 18),
      Student(1003, "mary", "F", 18),
      Student(1026, "alice", "M", 20),
      Student(1029, "kali", "F", 23)).toDS()

    val cl =spark.catalog
    //缓存DataSet/DataFrame
    cl.listFunctions().show(5000)
   /*
    //分组和透视的区别
    stus.groupBy("gender","age").count().show()
    stus.groupBy("gender").pivot("age").count().show()
    */
/*
    //自定义类型不安全的聚合函数
    val s = Seq("y", "e", "k")
    val fun = (name: String) => {
      val last = name.substring(name.length - 1)
      s.contains(last)
    }
    spark.udf.register("lastIsX", fun)
    stus.createTempView("students")
    spark.sql("select * from students where lastIsX(name)").show()
*/

    /*    //------select--------
        stus.select("id","name").show()
        stus.select($"id",$"name",$"age"+10).show()
        import org.apache.spark.sql.functions._
        stus.select(col("id"),col("name")).show()
        stus.select(stus("id"),stus("gender")).show()
        stus.selectExpr("id","name","age+10").show()
        //-------filter == where------
        stus.filter(stu=>stu.age>22).show()
        stus.filter("name in ('jack','alice')").show()
        stus.filter($"gender" === "M").show()
        //-------groupby-------------
        stus.groupBy("gender").count().show()
        stus.groupBy("gender").sum("age").show()
        val map=Map("age"->"sum","*"->"count")
        stus.groupBy("gender").agg(map).show()
        stus.groupBy("gender").agg("age"->"sum","age"->"count").show()
        //pivot 把未分组的列中的数据进行分组，并转置成列名,透视操作，观察趋势，观察某个指标的变化情况
        stus.groupBy("gender").count().show()
        stus.groupBy("gender").pivot("age").count().show()
        stus.groupBy("gender","age").count().show()
        //--------orderBy-----------
        stus.orderBy($"age" desc).show()
        //-------join--------------
        val scos=Seq(Score(1001,"语文",40),Score(1002,"数学",80),Score(1002,"英语",75),Score(2001,"物理",100))

        //stus.join(scos,stus("id") === scos("id"),"inner")
        //stus.join(scos,stus("id") === scos("id"),"inner")
    */
    spark.close()
  }


}

case class Person(name: String, age: Int)

case class Student(id: Int, name: String, gender: String, age: Int)

case class Score(id: Int, subject: String, score: Double)