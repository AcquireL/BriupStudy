package com.briup.base.exercise;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.*;

public class HbaseApi {
    private static Connection conn=null;
    private static Admin admin=null;
    private static Table table=null;
    private static ExecutorService pool= Executors.newFixedThreadPool(10);
    public static void getCon() throws IOException {
        Configuration conf= HBaseConfiguration.create ();
        conf.set ("hbase.zookeeper.qorum", "client:2181");
        pool= Executors.newFixedThreadPool (10);
        conn= ConnectionFactory.createConnection (conf,pool);
        admin=conn.getAdmin ();
        table=conn.getTable (TableName.valueOf ("bd1902:student"));
        System.out.println ("连接成功！");
    }
    //创建表
    public static void createTabel(String name) throws IOException, InterruptedException, ExecutionException, TimeoutException {
        TableDescriptorBuilder tbuilder = TableDescriptorBuilder.newBuilder (TableName.valueOf (name));
        ColumnFamilyDescriptorBuilder cbuilder = ColumnFamilyDescriptorBuilder.newBuilder (Bytes.toBytes ("baseinfo"));
        cbuilder.setMaxVersions (5);
        ColumnFamilyDescriptor cfd = cbuilder.build ();
        tbuilder.setColumnFamily (cfd);
        TableDescriptor tdes = tbuilder.build ();
        Future<Void> cb = admin.createTableAsync (tdes);
        Void aVoid = cb.get (10000, TimeUnit.MILLISECONDS);
        System.out.println ("创建表成功:"+name);
    }
    public static void main(String[] args) throws Exception {
        getCon ();
        createTabel ("apiTable");
    }
}
