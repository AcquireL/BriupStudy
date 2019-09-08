package com.briup.jdbc;

import java.sql.*;

public class HiveJDBCTest {
    private static Connection conn=null;
    private static Statement statement=null;
    //获得连接
    public static void getConn(){
        Driver driver=new org.apache.hive.jdbc.HiveDriver();
        try {
            DriverManager.deregisterDriver (driver);
            conn=DriverManager.getConnection ("jdbc:hive2://master:10000/bd1902","hdfs","713181");
            statement=conn.createStatement ();
            System.out.println ("连接成功："+conn);
        } catch (SQLException e) {
            e.printStackTrace ();
        }
    }
    //创建表
    public static void createTable(){
        String sql="create table t_phone_jdbc(id int,name string,price double) row format delimited fields terminated by ',' stored as textfile";
        try {
            boolean execute = statement.execute (sql);
            System.out.println ("t_tabel_jdbc创建成功：");
        } catch (SQLException e) {
            e.printStackTrace ();
        }
    }
    public static void main(String[] args) throws Exception {
        getConn ();
        //createTable ();
      /*  //注册驱动
        Class.forName ("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection ("jdbc:hive2://master:10000/bd1902","hdfs","713181");
        System.out.println (conn);
        Statement stat = conn.createStatement ();
        String sql="create table t_phone_jdbc(id int,name string,price double) row format delimited fields terminated by ',' stored as textfile";
        boolean flag = stat.execute (sql);
        System.out.println ("t_tabel_jdbc创建成功");
        conn.close ();*/
    }
}
