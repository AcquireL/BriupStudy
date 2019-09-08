package com.briup.sql

import org.apache.log4j.{Level, Logger}

case class Stu(id:Int,name:String,classid:Int,score:Double)
object WindowFunTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark=getSpark("Window")
    import spark.implicits._

    val s = Seq(Stu(1,"张三1",1,88.0D),
      Stu(4,"张三4",2,78.0D),
      Stu(3,"张三3",1,79.0D),
      Stu(8,"张三8",1,88.0D),
      Stu(5,"张三5",2,85.0D),
      Stu(2,"张三2",2,85.0D),
      Stu(6,"张三6",1,89.0D),
      Stu(7,"张三7",2,90.0D)).toDS
    s.createTempView("scores")
    //rank  row_number avg
/*    spark.sql("select id,name,classid,score,rank() " +
      "over(partition by classid order by score desc) rank, row_number() " +
      "over(partition by classid order by score desc) from scores").show()*/

    spark.sql("select id,name,classid,score," +
      "rank() over(partition by classid order by score desc) rank," +
      "row_number() over(partition by classid order by score desc) row_number," +
      "avg(score) over(partition by classid) avg,"+
      "avg(score) over(partition by classid order by score desc) avg_order,"+
      "sum(score) over(partition by classid) sum,"+
      "sum(score) over(partition by classid order by score desc) sum_order,"+
      "avg(score) over(partition by classid rows  between 1 preceding and 1 following) avg1,"+
      "avg(score) over(partition by classid order by score range  between 1 preceding and 1 following) avg2 "+
      "from scores").show()
    spark.close();
  }
}
