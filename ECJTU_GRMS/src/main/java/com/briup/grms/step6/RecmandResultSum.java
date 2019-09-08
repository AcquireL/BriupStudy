package com.briup.grms.step6;

import com.briup.grms.step1.PurchasedGoodsList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.stream.StreamSupport;


/**
 * 把step5计算结过果求和
 * gid,uid v
 */
public class RecmandResultSum<reduce> extends Configured implements Tool {
    public static void main(String[] arg)throws  Exception{
        ToolRunner.run (new RecmandResultSum (),arg);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        Job job = Job.getInstance(conf,"purchasedGoodsList");
        job.setJarByClass(this.getClass ());
        //为job装配mapper



        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //为job装配reducer
        job.setReducerClass(RRSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path (conf.get("inpath")));
        TextOutputFormat.setOutputPath(job, new Path(conf.get("outpath")));

        return job.waitForCompletion(true)?0:1;

    }
    public static class RRSReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer sum = StreamSupport.stream (values.spliterator (), false).map (s -> Integer.parseInt (s.toString ())).reduce ((x, y) -> x + y).get ();
            context.write (key,new Text(sum+""));
        }
    }


}
