package com.briup.Hbase_Api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class Api {
    private static Connection conn=null;
    private static Admin admin=null;
    private static Table table=null;
    public static  void getCon() throws IOException {
        Configuration conf = HBaseConfiguration.create ();
        conf.set ("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
        //conf.set("hbase.zookeeper.quorum","master:2181,slave:2181");
        Connection conn = ConnectionFactory.createConnection (conf);
        System.out.println ("连接成功："+conn);
        admin=conn.getAdmin ();
        //table=conn.getTable (TableName.valueOf ("lwj:InvertIndex_table"));
        table=conn.getTable(TableName.valueOf ("lwj:CleanDataMR"));
    }
    //创建命名空间 lwj
    public static void createNamespace() throws IOException {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create ("lwj");
        NamespaceDescriptor namespaceDescriptor = builder.build ();
        admin.createNamespace (namespaceDescriptor);
        System.out.println ("命名空间创建成功");
    }
    //创建CleanDataMR表
    //family useinfo，il，ol
    //qualifier url，title
    //创建rankesult表
    //family        info
    //qualifier     rank
    public  static void createTable() throws IOException {
        //创建CleanDataMR
   /*     HTableDescriptor hTableDescriptor=new HTableDescriptor(Bytes.toBytes ("lwj:CleanDataMR"));
        HColumnDescriptor hColumnDescriptor=new HColumnDescriptor (Bytes.toBytes ("useinfo"));
        HColumnDescriptor hColumnDescriptor1=new HColumnDescriptor (Bytes.toBytes ("il"));
        HColumnDescriptor hColumnDescriptor2=new HColumnDescriptor (Bytes.toBytes ("ol"));
        hTableDescriptor.addFamily (hColumnDescriptor);
        hTableDescriptor.addFamily (hColumnDescriptor1);
        hTableDescriptor.addFamily (hColumnDescriptor2);
        admin.createTable (hTableDescriptor);
        System.out.println ("创建成功")*/;
        //创建InvertIndex_table
        HTableDescriptor hTableDescriptor1=new HTableDescriptor (Bytes.toBytes ("lwj:InvertIndex_table"));
        HColumnDescriptor hColumnDescriptor5=new HColumnDescriptor (Bytes.toBytes ("page"));
        hTableDescriptor1.addFamily (hColumnDescriptor5);
        admin.createTable (hTableDescriptor1);
        System.out.println ("创建成功");
    }
    //删除表
    public static void  dropTable() throws IOException {
   /*     admin.disableTable (TableName.valueOf ("lwj:CleanDataMR"));
        admin.deleteTable (TableName.valueOf ("lwj:CleanDataMR"));*/
        admin.disableTable (TableName.valueOf ("lwj:InvertIndex_table"));
        admin.deleteTable (TableName.valueOf ("lwj:InvertIndex_table"));
        System.out.println ("删除成功");
    }
    //get
    public static void select() throws IOException {
        Get get = new Get(Bytes.toBytes
                ("com.securityweek.www:https/critical-vulnerabilities-patched-apache-couchdb"));
        get.setMaxVersions();
        Result result = table.get(get);
        showResult(result);
    }
    //scan
    public static void selectWhere() throws IOException {
        Scan scan=new Scan();
        ResultScanner scanner = table.getScanner (scan);
        int count=0;
        for(Result rs:scanner){
            count++;
            showResult (rs);
        }
        System.out.println (count);
    }
    public static int test=0;
    //显示查询结果
    public static void showResult(Result result){
        //System.out.println ("----------------------");
        //System.out.println ("结果中的第一行数据："+Bytes.toString (result.value ()));
     System.out.println ("行键："+Bytes.toString (result.getRow ()));
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap ();
        for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> qfv:map.entrySet ()){
            System.out.println ("列族名为："+new String (qfv.getKey ()));
            for(Map.Entry<byte[],NavigableMap<Long, byte[]>> v:qfv.getValue ().entrySet ()){
                System.out.println ("列名为："+new String(v.getKey ()));
                for(Map.Entry<Long, byte[]> data:v.getValue ().entrySet ()){
                    System.out.println ("结果："+Bytes.toString (data.getValue ()));
                }
            }
        }
  /*      byte[] keyword_byte = result.getValue (Bytes.toBytes ("useinfo"), Bytes.toBytes ("keyword"));
        byte[] url_byte= result.getValue (Bytes.toBytes ("useinfo"), Bytes.toBytes ("url"));
        byte[] rank_byte= result.getValue (Bytes.toBytes ("useinfo"), Bytes.toBytes ("rank"));
        String keyword=null;
            keyword=Bytes.toString (keyword_byte).replace (" ","");
        String url=Bytes.toString (url_byte);
        double rank=Bytes.toDouble (rank_byte);

        if(keyword!=null){
            test++;
            System.out.println (keyword);
            System.out.println (url);
            System.out.println (rank);
        }*/

    /*    for(Cell cell:result.rawCells ()){
            if(Bytes.toString (CellUtil.cloneFamily (cell)).equals ("ol")){
                System.out.println (Bytes.toString (CellUtil.cloneQualifier (cell)));
            }

            if(Bytes.toString (CellUtil.cloneFamily (cell)).equals ("useinfo")&&Bytes.toString (CellUtil.cloneQualifier (cell)).equals ("rank")){
                System.out.println (Bytes.toDouble (CellUtil.cloneValue (cell)));
            }
        }*/
       /* byte[] value = result.getValue (Bytes.toBytes ("f"), Bytes.toBytes ("st"));
        System.out.println (Bytes.toInt (value)==2);*/
       /* Cell fetched_status = result.getColumnLatestCell (Bytes.toBytes ("f"), Bytes.toBytes ("st"));
        byte[] f_s_value = Arrays.copyOfRange
                (fetched_status.getValueArray(),
                        fetched_status.getValueOffset(),
                        fetched_status.getValueArray().length) ;
        Bytes.toInt (CellUtil.cloneValue (fetched_status));
        System.out.println ((Bytes.toInt (CellUtil.cloneValue (fetched_status)) == 2) +"  "+count++);*/
        /*String ilcon=null;
        String title=null;
        Double rank=null;
        for(Cell cell:result.rawCells ()){
            if(Bytes.toString (CellUtil.cloneFamily (cell)).equals ("useinfo")&&Bytes.toString (CellUtil.cloneQualifier (cell)).equals ("ilcon")){
                ilcon=Bytes.toString (CellUtil.cloneValue (cell));
            }
            if(Bytes.toString(CellUtil.cloneFamily (cell)).equals ("useinfo")&&Bytes.toString(CellUtil.cloneQualifier (cell)).equals ("title")){
                title=Bytes.toString (CellUtil.cloneValue (cell));
            }
            String keyword=ilcon+title;
            if(Bytes.toString(CellUtil.cloneFamily (cell)).equals ("useinfo")&&Bytes.toString(CellUtil.cloneQualifier (cell)).equals ("rank")){
                rank=Bytes.toDouble (CellUtil.cloneValue (cell));
            }
            if(keyword!=null&&rank!=null){
                System.out.println (keyword+""+rank);
            }
        }*/
    }
    //put
    public static void put() throws IOException {
        Put put=new Put (Bytes.toBytes ("lwj:InvertIndex_table"));
        put.addColumn (Bytes.toBytes ("info"), Bytes.toBytes ("owner"), Bytes.toBytes ("lwj"));
        table.put (put);
    }
    public static void main(String[] args) throws IOException {

        getCon ();
        //createNamespace ();
        dropTable ();
        createTable ();
        //select ();
        //put ();
        //selectWhere ();
    }
}

/*
行键：com.securityweek.www:https/critical-vulnerabilities-patched-apache-couchdb
        0.15982352608362202*/

/*行键：com.securityweek.www:https/critical-vulnerabilities-patched-apache-couchdb
        0.15982352608362202
        0.15999631879258674
        0.16037379257517034
        0.16120525811676611*/
