package com.briup.base.exercise;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.*;


public class ConnectHbase {
    private  static ExecutorService pool=null;
    public static Connection conn=null;
    public static AsyncConnection conn1=null;
    //同步获取连接对象
    public static void getCon() throws IOException {
        Configuration conf= HBaseConfiguration.create ();
        conf.set ("hbase.zookeeper.qorum", "client:2181");
        pool= Executors.newFixedThreadPool (10);
        conn= ConnectionFactory.createConnection (conf,pool);
        System.out.println ("连接成功！");
    }
    //异步获取连接对象
    public static void getCon_ansy() throws ExecutionException, InterruptedException, TimeoutException {
        Configuration conf=HBaseConfiguration.create ();
        conf.set("hbase.zookeeper.quorum","client:2181");
        //异步连接获取回调对象
        CompletableFuture<AsyncConnection> cb = ConnectionFactory.createAsyncConnection (conf);
        System.out.println ("操作1");
        System.out.println ("操作2");
        System.out.println ("操作3");
        conn1 = cb.get (10000, TimeUnit.MILLISECONDS);
        System.out.println ("异步获连接成功："+conn1);
    }
    //异步创建表
    public static void create(String name) throws Exception {
        Admin admin=conn.getAdmin();
        TableDescriptorBuilder tbuilder = TableDescriptorBuilder.newBuilder (TableName.valueOf (name));
        ColumnFamilyDescriptorBuilder cbuilder = ColumnFamilyDescriptorBuilder.newBuilder (Bytes.toBytes ("baseinfo"));
        cbuilder.setMaxVersions (5);
        ColumnFamilyDescriptor cfd = cbuilder.build ();
        tbuilder.setColumnFamily (cfd);
        TableDescriptor tdes = tbuilder.build ();
        byte[][] p=new byte[][]{
                Bytes.toBytes ("1000"),
                Bytes.toBytes ("2000"),
                Bytes.toBytes ("3000")};
        Future<Void> cb = admin.createTableAsync (tdes, p);
        Void aVoid = cb.get (10000, TimeUnit.MILLISECONDS);
        System.out.println ("创建表成功:"+name);
    }

    public static void main(String[] args) throws Exception{
        getCon ();
        //getCon_ansy();
        System.out.println (conn);
        //System.out.println (conn1);
        create ("java_api:student2");
    }
}
