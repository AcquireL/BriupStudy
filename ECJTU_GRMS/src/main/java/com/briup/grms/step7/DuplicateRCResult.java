package com.briup.grms.step7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 *
 * 去重
 * 从用户推荐列表中，剔除用户已购商品
 *
 * 连接  MultipleInputs
 *
 * 或者使用TextInputFormat.addInputPaths()从两个文件中读取数据
 */
public class DuplicateRCResult extends Configured implements Tool {
    public  static void main(String[] args) throws Exception{
        ToolRunner.run (new DuplicateRCResult (),args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf ();
        Job job= Job.getInstance (conf,"duplicateRCResult");
        job.setJarByClass (this.getClass ());

        MultipleInputs.addInputPath (job,new Path (conf.get ("inpath1")), TextInputFormat.class,ReadODMapper.class);
        MultipleInputs.addInputPath (job,new Path(conf.get ("inpath2")), KeyValueTextInputFormat.class,Mapper.class);

        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (Text.class);

        job.setReducerClass (DRRReducer.class);
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (Text.class);

        job.setOutputFormatClass (TextOutputFormat.class);
        TextOutputFormat.setOutputPath (job,new Path (conf.get ("outpath")));

        return job.waitForCompletion (true)?0:1;
    }
    // uid  gid  1-> gid,uid 1
    public static class ReadODMapper extends Mapper<LongWritable,Text,Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] info=value.toString ().split (" ");
            String pair=info[1]+","+info[0];
            context.write (new Text (pair),new Text (info[2]));
        }
    }
    //第二个mapper直接用keyValueInputFormate原样输出

    public static class DRRReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> value=values.iterator ();
            String v1=value.next ().toString ();
            if(value.hasNext ()){
                return;
            }
            String[] gus=key.toString ().split (",");
            context.write (new Text (gus[0]+"\t"+gus[1]),new Text (v1));
        }
    }
}
