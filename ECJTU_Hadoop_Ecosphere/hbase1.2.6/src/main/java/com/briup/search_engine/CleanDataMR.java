package com.briup.search_engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;


/**
 * 输入： 172.16.0.4:16010    webpage_aliyun/webpage_huwei
 * zk:172.16.0.4, 172.16.0.5,172.16.0.6,172.16.0.7
 *
 * 输出：名字_类名 lwj_CleanDataMR
 * 数据清洗
 * 从抓取过的页面中（f:st=2)
 * 提取有效字段(url，title，il，ol)
 */
public class CleanDataMR extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run (new CleanDataMR (), args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf ();
        conf.set("hbase.zookeeper.quorum","172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
        //conf.set ("hbase.zookeeper.quorum", "master:2181,slave:2181");
        Job job=Job.getInstance (conf, "lwj_clean");
        job.setJarByClass (this.getClass ());
        TableMapReduceUtil.initTableMapperJob ("aliyun_webpage",new Scan (), CleanDataMapper.class, ImmutableBytesWritable.class, Put.class,job);
        TableMapReduceUtil.initTableReducerJob ("lwj:CleanDataMR", CleanDataReduce.class, job);
        return job.waitForCompletion (true)?0:1;
    }
    public static class CleanDataMapper extends TableMapper<ImmutableBytesWritable,Put>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Put put =new Put(key.get ());
            Cell columnLatestCell = value.getColumnLatestCell (Bytes.toBytes ("f"), Bytes.toBytes ("st"));
            byte[] st = CellUtil.cloneValue (columnLatestCell);
            if(Bytes.toInt (st)==2 || Bytes.toInt (st)==5){
                //baseUrl
                byte[] base_url=getValueByFamilyAndQualifier (value, Bytes.toBytes ("f"), Bytes.toBytes ("bas"));
                put.addColumn (Bytes.toBytes ("useinfo"), Bytes.toBytes ("url"), base_url);
                //title
                byte[] page_title =
                        getValueByFamilyAndQualifier
                                (value,Bytes.toBytes("p"),Bytes.toBytes("t"));
                if(page_title!=null){
                    put.addColumn (Bytes.toBytes ("useinfo"), Bytes.toBytes ("title"), page_title);
                }
                //入链接个数
                int ol_num = countQualifierNum(value,Bytes.toBytes("ol"));
                put.addColumn (Bytes.toBytes ("useinfo"),Bytes.toBytes ("oln"),Bytes.toBytes (ol_num));
                //入链接内容
                //拿到 il列族下的所有 列 和 值组成的map
                NavigableMap<byte[], byte[]> qvs = value.getFamilyMap(Bytes.toBytes("il"));
                //拿到当前页面的title值
                byte[] title= value.getValue(Bytes.toBytes("p"), Bytes.toBytes("t"));
                //如果 map长度大于0代表该页面有入链
                if(qvs.size() > 0){
                    Set<Map.Entry<byte[], byte[]>> qvset = qvs.entrySet();
                    //拿到第一个入链的 内容 作为基础值
                    byte[] v = qvs.firstEntry().getValue();
                    //遍历所有的入链内容，把第一个非空值作为 keyword
                    for (Map.Entry<byte[], byte[]> e : qvset) {
                        v = e.getValue();
                        if(v.length > 0){
                            break;
                        }
                    }
                    String keyword=(Bytes.toString(v) +"\t"+Bytes.toString(title));
                    // 把入链内容和title共同作为关键字
                    if(title != null){
                        put.addColumn (Bytes.toBytes ("useinfo"), Bytes.toBytes ("keyword"), Bytes.toBytes (keyword));
                    }else {
                        put.addColumn (Bytes.toBytes ("useinfo"), Bytes.toBytes ("keyword"), v);
                    }
                }
                //出链接
                NavigableMap<byte[], byte[]> qvso = value.getFamilyMap(Bytes.toBytes ("ol"));
                for (Map.Entry<byte[], byte[]> qv : qvso.entrySet()) {
                    byte[] q = qv.getKey();
                    byte[] v = qv.getValue();
                    put.addColumn (Bytes.toBytes ("ol"), q, v);
                }
                context.write (key, put);
            }
        }
        private byte[] getValueByFamilyAndQualifier(Result value, byte[] family, byte[] qualifier) {
            Cell cell = value.getColumnLatestCell
                    (family, qualifier);
            if(cell == null) {
                return Bytes.toBytes(0);
            }
            return Arrays.copyOfRange
                    (cell.getValueArray(),
                            cell.getValueOffset(),
                            cell.getValueArray().length) ;
        }
        private int countQualifierNum(Result value, byte[] family) {
            NavigableMap<byte[], byte[]> qvs = value.getFamilyMap(family);
            return qvs.size();
        }
    }
    public static class CleanDataReduce extends TableReducer<ImmutableBytesWritable, Put, NullWritable>{
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            context.write (NullWritable.get (), values.iterator ().next ());
        }
    }
}
