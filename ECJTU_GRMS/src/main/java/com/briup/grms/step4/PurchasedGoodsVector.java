package com.briup.grms.step4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * 获取用户购买向量的  顺时针旋转90度结果
 * 输入 /data/rmc/process/matrix-data.txt
 * 输出 grms/step4
 * 10001 20001
 * 10001 20005
 *
 *结果
 * 20001 10001:1 10002:1......
 * 20002 .......
 *
 */
public class PurchasedGoodsVector extends Configured implements Tool {
    public static void main(String [] args)throws Exception{
        ToolRunner.run (new PurchasedGoodsVector (),args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf ();
        Job job=Job.getInstance (conf,"PurchasedGoodsVector");
        job.setJarByClass (PurchasedGoodsVector.class);

        job.setMapperClass (PGVMapper.class);
        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (Text.class);

        job.setReducerClass (PGVReducer.class);
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (Text.class);

        job.setInputFormatClass (TextInputFormat.class);
        TextInputFormat.addInputPath (job,new Path (conf.get ("inpath")));
        job.setOutputFormatClass (TextOutputFormat.class);
        TextOutputFormat.setOutputPath (job,new Path (conf.get ("outpath")));

        return job.waitForCompletion (true)?0:1;
    }
    public static class PGVMapper extends Mapper<LongWritable,Text,Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos=value.toString ().split (" ");
            String uid=infos[0];
            String gid=infos[1];
            context.write (new Text (gid),new Text (uid));
        }
    }
    public static class PGVReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer vector =new StringBuffer ();
            //10001:1,10002:1,....
            values.forEach (s->vector.append (s.toString ()).append (":").append (1).append (","));
            String v=vector.substring (0,vector.length ()-1);
            context.write (key,new Text (v));
        }
    }
}
