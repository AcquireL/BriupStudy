package com.briup.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.*;


public class ApiTest {
    private static Connection conn=null;
    private static Admin admin=null;
    private static Table table=null;
    private static AsyncTable<AdvancedScanResultConsumer> table_asyn=null;
    private static AsyncAdmin admin_asyn=null;
    //java提供的线程池
    private static ExecutorService pool= Executors.newFixedThreadPool(10);
    //同步获取连接
    public static void getConnection() throws IOException {
        //1 获取hbase配置对象
        Configuration conf= HBaseConfiguration.create ();
        //2 增加配置项
        // 写java代码的主机的hosts文件中增加hbase机器的ip
        conf.set("hbase.zookeeper.quorum","client:2181");
        //3 获得连接对象
        conn= ConnectionFactory.createConnection (conf,pool);
        System.out.println ("连接成功："+conn);
        //4 获得句柄对象
        admin=conn.getAdmin ();
        table=conn.getTable (TableName.valueOf ("java_api:employee"));
    }
    //异步获取连接
    public static void getConnection_asyn() throws Exception {
        Configuration conf=HBaseConfiguration.create ();
        conf.set("hbase.zookeeper.quorum","client:2181");
        //异步连接获取回调对象
        CompletableFuture<AsyncConnection> cb = ConnectionFactory.createAsyncConnection (conf);
        System.out.println ("操作1");
        System.out.println ("操作2");
        System.out.println ("操作3");
        AsyncConnection conn = cb.get (10000, TimeUnit.MILLISECONDS);
        System.out.println ("异步获连接成功："+conn);
        admin_asyn=conn.getAdmin (pool);
        table_asyn = conn.getTable (TableName.valueOf ("java_api:employee"));
    }

    public static void closeConn() throws IOException{
        admin.close( );
        table.close ();
        conn.close ();
    }
    //创建命名空间，创建表（多个列族，不同版本号，预分区），增，查（使用结果的各种方法）
    //hbase api 构建对象方式  大多数都是用的建造者模式
    public static void createNS(String name) throws IOException {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create (name);
        NamespaceDescriptor desc = builder.build ();
        admin.createNamespace (desc);
        System.out.println ("同步创建成功");
    }
    //异步创建表空间
    public static void createNS_asyn(String name) throws Exception{
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create (name);
        NamespaceDescriptor desc = builder.build ();
        Future<Void> cb = admin.createNamespaceAsync (desc);
        Void aVoid = cb.get (10000, TimeUnit.MILLISECONDS);
        System.out.println ("异步创建成功");
    }
    //异步创建表
    public static void create(String name) throws Exception {
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
    //put
    public  static void insert() throws Exception {
        table=conn.getTable (TableName.valueOf ("java_api:employee"));
        Put put=new Put (Bytes.toBytes ("1001"));
        put.addColumn (Bytes.toBytes ("baseinfo"), Bytes.toBytes ("name"), Bytes.toBytes ("Terry"));
        put.addColumn (Bytes.toBytes ("baseinfo"), Bytes.toBytes ("age"), Bytes.toBytes ("36"));
        table.put (put);
    }
    //get
    public static void  select() throws Exception {
        Get get=new Get (Bytes.toBytes ("1001"));
        //可以理解成数组 Cell
        Result result = table.get (get);
        //CompletableFuture<Result> resultCompletableFuture = table_asyn.get (get);
        showResult (result);
    }
    //scan 过滤器
    public  static void selectWhere() throws Exception {
        Scan scan =new Scan();
   /*     ResultScanner rs = table.getScanner (scan);
        for(Result r:rs){
            showResult(r);
        }*/
        //1 行建过滤器   2 列族过滤器
        //3 列名过滤器   3 值过滤器
        RowFilter rowFilter = new RowFilter (CompareOperator.GREATER, new BinaryComparator (Bytes.toBytes ("20000")));
        scan.setFilter (rowFilter);
        RandomRowFilter f = new RandomRowFilter (0.5f);
        scan.setFilter (f);
    }
    public static void showResult(Result result){
        System.out.println ("结果中的cell个数："+result.size ());
        System.out.println ("结果是否包含某列："+result.containsColumn (Bytes.toBytes ("java_api:baseinfo"), Bytes.toBytes ("name")));
        System.out.println ("结果中是否包含某个空列："+result.containsEmptyColumn (Bytes.toBytes ("java_api:baseinfo"), Bytes.toBytes ("name")));
        System.out.println ("获得某列中的数据："+new String(result.getValue (Bytes.toBytes ("baseinfo"), Bytes.toBytes ("name"))));
        System.out.println ("结果中的第一行数据："+Bytes.toString (result.value ()));
        System.out.println ("-----------------------------");
        //查找列族
        NavigableMap<byte[], byte[]> map1 = result.getFamilyMap (Bytes.toBytes ("java_api:baseinfo"));
        map1.forEach ((k,v)->{
            System.out.println (Bytes.toString (k)+"  "+Bytes.toString (v));
        });
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cqtvs = result.getMap ();

        for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cqtv:cqtvs.entrySet ()){
            byte[] c=cqtv.getKey ();
            NavigableMap<byte[], NavigableMap<Long, byte[]>> qtvs=cqtv.getValue ();
            for(Map.Entry<byte[], NavigableMap<Long, byte[]>> qtv:qtvs.entrySet () ){
                byte[] qt=qtv.getKey ();
                NavigableMap<Long, byte[]> tvs=qtv.getValue ();
                for(Map.Entry<Long,byte[]> tv:tvs.entrySet ()){
                    Long v=tv.getKey ();
                    byte[] value=tv.getValue ();
                    System.out.println ("列族为："+new String(c)+"  列名："+new String (qt)+"  值："+new String (value) );
                }
            }
        }
        System.out.println ("============================");
    }

    public static void main(String[] args) throws Exception {
        getConnection();
        //getConnection_asyn();
        //createNS("java_api");
        //createNS_asyn ("java_api_asyn");
        //create ("java_api:employee");
        //insert ();
        select ();
       closeConn ();
    }
}
