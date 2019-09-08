package com.briup.grms.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;

/**
 * 计算两两商品的共现次数
 * 输入数据  ./grms/setp1
 * 输入数据  ./grms/setp2
 * 10001     20001，20002，20003....
 *
 * 20001    20002   3
 * 20002    20004   6
 *
 *
 * 思考？
 * 下一步我们需要把共现列表整理成矩阵形式
 * 考虑到这一步，怎么做能更方便下一步操作
 *
 */
public class GoodsCooccurrenceList extends Configured implements Tool {
    public static void main(String[] args)throws Exception{
        ToolRunner.run (new GoodsCooccurrenceList (),args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf ();
        Job job=Job.getInstance (conf,"GoodCooccurrenceList");
        job.setJarByClass (GoodsCooccurrenceList.class);

        job.setMapperClass (GCLMapper.class);
        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (NullWritable.class);

        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (NullWritable.class);

        job.setInputFormatClass (TextInputFormat.class);
        job.setOutputFormatClass (TextOutputFormat.class);
        TextInputFormat.addInputPath (job,new Path (conf.get ("inpath")));
        TextOutputFormat.setOutputPath (job,new Path(conf.get ("outpath")));

        return job.waitForCompletion (true)?0:1;
    }
    public static class GCLMapper extends Mapper<LongWritable, Text,Text,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { ;
            String[] infos=value.toString ().split ("[\t]");
            String[] goods=infos[1].split ("[,]");
            for(String good1:goods){
                for (String good2:goods){
                    context.write (new Text (good1+"\t"+good2),NullWritable.get ());
                }
            }
        }
    }

}
