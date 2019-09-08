package com.briup.streaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

object MassageServer {

  // 定义随机获取整数的方法
  def index(length: Int) = {
    import java.util.Random
    val rdm = new Random
    rdm.nextInt(length)
  }

  def main(args: Array[String]) {
    println("模拟数据器启动！！！")
    // 获取指定文件总的行数
    val filename ="Spark/files/ihaveadream.txt";
    val lines = Source.fromFile(filename).getLines.toList
    val filerow = lines.length

    // 指定监听某端口，当外部程序请求时建立连接
    val serversocket = new ServerSocket(9999);

    while (true) {
      //监听9999端口，获取socket对象
      val socket = serversocket.accept()
      //      println(socket)
      new Thread() {
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)

          val out = new PrintWriter(socket.getOutputStream(), true)

          while (true) {
            Thread.sleep(1000)
            // 当该端口接受请求时，随机获取某行数据发送给对方
            val content = lines(index(filerow))

            println (content)

            out.write(content + '\n')

            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
