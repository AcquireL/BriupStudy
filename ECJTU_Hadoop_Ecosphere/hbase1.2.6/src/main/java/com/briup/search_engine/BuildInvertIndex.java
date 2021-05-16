package com.briup.search_engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;



public class BuildInvertIndex extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run (new BuildInvertIndex (), args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf ();
        //conf.set ("hbase.zookeeper.quorum", "master:2181,slave:2181");
        conf.set ("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
        Job job= Job.getInstance (conf, "lwj_invertIndex");
        job.setJarByClass (this.getClass ());
        TableMapReduceUtil.initTableMapperJob ("lwj:CleanDataMR",new Scan (), InvertMapper.class, Text.class, Text.class,job);
        TableMapReduceUtil.initTableReducerJob ("lwj:InvertIndex_table",InvertReducer.class, job);
        return job.waitForCompletion (true)?0:1;
    }
    public static class InvertMapper extends TableMapper<Text,Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] keyword_byte = value.getValue (Bytes.toBytes ("useinfo"), Bytes.toBytes ("keyword"));
            byte[] url_byte= value.getValue (Bytes.toBytes ("useinfo"), Bytes.toBytes ("url"));
            byte[] rank_byte= value.getValue (Bytes.toBytes ("useinfo"), Bytes.toBytes ("rank"));
            String keyword=Bytes.toString (keyword_byte);
            String url=Bytes.toString (url_byte);
            double rank=Bytes.toDouble (rank_byte);
            if(keyword_byte!=null){
                context.write (new Text (keyword), new Text (url+","+rank));
            }
        }
    }
    public static class InvertReducer extends TableReducer<Text,Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try{
                Put put=new Put (Bytes.toBytes (key.toString ()));
                String s = values.iterator ().next ().toString ().trim ();
                String[] split = s.split ("[,]");
                String url=split[0];
                double rank=Double.parseDouble (split[1]);
                put.addColumn (Bytes.toBytes ("page"), Bytes.toBytes (url), Bytes.toBytes (rank));
                context.write (NullWritable.get (), put);
            }catch (Exception e){
                return;
            }

        }
    }
}
