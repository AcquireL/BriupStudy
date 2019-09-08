package com.briup.core

import java.sql.DriverManager

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.junit.Test

class MovieRDDTest {
  //统计看电影的男女比例，并存入myslq数据库
  @Test
  def first(): Unit = {
    val sc = getSC("first")
    val users = sc.textFile("hdfs://172.16.0.4:9000/data/grouplens/ml-1m/users.dat")
    //先计算男女用户个数，最大公约数
    val r: RDD[(Char, Int)] = users.flatMap(_.split("::")(1)).groupBy(x => x).mapValues(ite => ite.size)
    val r1: RDD[(String, Int)] = users.map(x => (x.split("::")(1), 1)).reduceByKey(_ + _)
    /* val r2: collection.Map[Char, Long] = users.flatMap(_.split("::")(1)).countByValue()
     var f_num=r2('F')
     var m_num=r2('M')
     //最大公约数
     var g=gcd(f_num,m_num)
     println("男女比例是："+m_num/g+"比"+f_num/g)*/

    // 将数据存入mysql
    // movie_user  gender number

    //会导致序列化序列化问题,重量级connection对象的创建问题
    /* r1.foreach(x => {
       val url = "jdbc:mysql://localhost:3306/briup?useSSL=false"
       val driver = "com.mysql.jdbc.Driver"
       val user = "root"
       val passwd = "713181"
       Class.forName(driver)
       val conn = DriverManager.getConnection(url, user, passwd)
       val sql = "insert into movie_user values(?,?)"
       val prep = conn.prepareStatement(sql)
       prep.setString(1, x._1)
       prep.setLong(2, x._2)
       prep.execute()
       prep.close()
       conn.close()
     })*/

    r1.foreachPartition(
      ite => {
        val url = "jdbc:mysql://localhost:3306/briup?useSSL=false"
        val driver = "com.mysql.jdbc.Driver"
        val user = "root"
        val passwd = "713181"
        Class.forName(driver)
        val conn = DriverManager.getConnection(url, user, passwd)
        val sql = "insert into movie_user values(?,?)"
        val prep = conn.prepareStatement(sql)
        ite.foreach(x => {
          prep.setString(1, x._1)
          prep.setLong(2, x._2)
          prep.execute()
        })
        prep.close()
        conn.close()
      }
    )
    sc.stop()
  }

  // 10   6
  //10 % 6 =1  4
  //6 %  4=1   2
  //4 $  2=2   0

  def gcd(x: Long, y: Long): Long = {
    var temp = x % y
    if (temp == 0) {
      y
    } else {
      gcd(y, temp)
    }
  }

  //统计某个文件的单词个数，并统计出来空行的个数
  @Test
  def second() = {
    val sc = getSC("second")
    val r = sc.textFile("files/info.txt")
    //var blank = 0
    var blank = sc.longAccumulator("blank")
    val wc = r.flatMap(x => {
      if (x == "") {
        //blank += 1
        blank.add(1)
      }
      x.split(" ")
    }).groupBy(x => x).mapValues(ite => ite.size)
    wc.foreach(println)
    //println("空行个数为："+blank)
    println("空行个数：" + blank.value)
    sc.stop()

    //闭包中所使用的局部变量，会发送到Worker
    //但是并不会把值返回给Driver
    //如果在闭包函数，做数字累加，
    //在Driver节点是看不到的
  }

  @Test
  def save() = {
    val sc = getSC("save")
    val rdd: RDD[(Int, Long)] = sc.parallelize((10 to 100 by 10)).zipWithIndex
    //rdd.saveAsObjectFile("objFile")
    //rdd.saveAsSequenceFile("seqFile")
    rdd.saveAsNewAPIHadoopFile("Hfile", classOf[Text], classOf[Text], classOf[TextOutputFormat[Text, Text]])
    sc.stop()
  }

  @Test
  def read() = {
    val sc = getSC("read")
    /* val rdd=sc.sequenceFile[Int,Long]("seqFile")
     rdd.foreach(println)*/
    val rdd1 = sc.newAPIHadoopFile[Text, Text, KeyValueTextInputFormat]("Hfile")
    rdd1.foreach(println)
    sc.stop()
  }

  @Test
  def avgScore() = {
    //计算每个用户的平均评分， 分数最高 10 分数最低的 10
    val sc = getSC("avgScore")
    //整理数据  提取有效字段
    //    val users = sc.textFile("hdfs://172.16.0.4:9000/data/grouplens/ml-1m/users.dat")
    //1::1193::5::978300760
    val ratings = sc.textFile("hdfs://172.16.0.4:9000/data/grouplens/ml-1m/ratings.dat")
    val user_score = ratings.map(line => {
      val Array(_, userid, score, _) = line.split("::")
      (userid, score.toDouble)
    })
    //------------------------------
    /*val user_avg: RDD[(String, Double)] =user_score.groupByKey().mapValues(ite=>{
      val l=ite.toList
      l.sum/l.size
    })*/
    val user_avg = user_score.combineByKey[(Double, Long)](
      (v: Double) => (v, 1L),
      (e: (Double, Long), v: Double) => (e._1 + v, e._2 + 1),
      (e1: (Double, Long), e2: (Double, Long)) => (e1._1 + e2._1, e1._2 + e2._2)
    ).mapValues(e => e._1 / e._2)

    user_avg.map {
      case (x, y) => (y, x)
    }.top(10).foreach(println)
  }

  @Test
  def pageRank() = {
    val sc = getSC("pageRank")
    /* //page ol
     val page_ol=sc.newAPIHadoopFile[Text,Text,KeyValueTextInputFormat]("hdfs://172.16.0.4:9000/data/spark/pageRank/graphx-wiki-edges.txt").groupByKey()
     //page,1
     val page_rank =sc.newAPIHadoopFile[Text,Text,KeyValueTextInputFormat]("hdfs://172.16.0.4:9000/data/spark/pageRank/graphx-wiki-vertices.txt").map(x=>(x._1,1.0))
     val join: RDD[(Iterable[Text], Double)] =page_ol.join(page_rank).flatMap(ite=>{
      val l=ite._2._1.toList
       val size=l.size
       ite._2._1.map(x=>{
         (ite._2._1,ite._2._2/size)
       })
     })*/


    val page_ol = sc.textFile("hdfs://172.16.0.4:9000/data/spark/pageRank/graphx-wiki-edges.txt").map(line => {
      val Array(page, ol) = line.split("\t")
      (page, ol)
    })

    var page_rank: RDD[(String, Double)] = sc.textFile("hdfs://172.16.0.4:9000/data/spark/pageRank/graphx-wiki-vertices.txt").map(line => {
      val Array(page, title) = line.split("\t")
      (page, 1.0)
    })
    val page_ols = page_ol.groupByKey()


    1 to 50 foreach (x => {
      page_rank = page_rank.join(page_ols).flatMap {
        case (page, (rank, ols)) => {
          val l = ols.toList
          val s = l.size
          ols.map(ol => (ol, rank / s))
        }
      }.reduceByKey(_ + _).mapValues(_ * 0.85 + 0.15)
    })

    page_rank.map(x=>(x._2,x._1)).top(10).foreach(println)

    Thread.sleep(1000)
    sc.stop()
  }
}
