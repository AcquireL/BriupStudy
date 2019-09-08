package com.briup.base.exercise;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CreateTable extends ConnectHbase{
    public static void createTable_syn(String name,String clu) throws IOException {
        System.out.println (conn);
        Admin admin = conn.getAdmin ();
        HTableDescriptor ht = new HTableDescriptor (TableName.valueOf (name));
        HColumnDescriptor hc = new HColumnDescriptor (clu);
        ht.addFamily (hc);
        admin.createTable (ht);
        System.out.println ("创建成功");
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
    public static void main(String[] args) throws Exception {
        getCon ();
        //createTable_syn ("java_api:student2", "baseinfo");
        create ("java_api:student2");
    }
}
