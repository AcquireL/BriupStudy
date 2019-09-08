package com.briup.grms.step5;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.util.Iterator;


/**
 *
 * 1.计算对于某个用户推荐某个商品的推荐值
 * 输入  step3结果     step4结果
 * 进行连接操作   reduce端连接
 * 借助 MultipleInputs 类把两个Mapper输出的结果汇聚到同一个reduce
 * 2.二次排序
 *      i 构建复合键  Comparable 即比较自然键，也比较自然值
 *      ii 分区比较器 只比较复合建中的自然键
 *      iii 分组比较器  只比较复合建中的自然键
 *
 * 结果 g1,u1  1
 *      g1.u2  2
 *      g1,u3  1
 *      g2,u2   1
 *      g2,u2   3
 */
public class GetRecmandResult extends Configured implements Tool {
    public static void main(String [] args)throws Exception{
        ToolRunner.run (new GetRecmandResult (),args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf ();
        Job job=Job.getInstance (conf,"GetRecmandResult");
        job.setJarByClass (this.getClass ());

        MultipleInputs.addInputPath (job,new Path (conf.get ("inpath1")),KeyValueTextInputFormat.class,ReadGCMatrixMapper.class);
        MultipleInputs.addInputPath (job,new Path (conf.get ("inpath2")),KeyValueTextInputFormat.class,ReadPGVetorMapper.class);

        job.setMapOutputKeyClass (IdFlag.class);
        job.setMapOutputValueClass (Text.class);

        job.setPartitionerClass (IdFlagPartitioner.class);
        job.setGroupingComparatorClass (IdFlagGroupingComparator.class);


        job.setReducerClass (GRRReducer.class);
        job.setOutputKeyClass (Text.class);
        job.setOutputValueClass (Text.class);

        job.setOutputFormatClass (TextOutputFormat.class);
        TextOutputFormat.setOutputPath (job,new Path(conf.get ("outpath")));

        return job.waitForCompletion (true)?0:1;
    }
    public static class ReadGCMatrixMapper extends Mapper<Text,Text,IdFlag,Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            IdFlag id=new IdFlag (key,new IntWritable(0));
            context.write (id,value);
        }
    }
    public static class ReadPGVetorMapper extends Mapper<Text,Text,IdFlag,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            IdFlag id=new IdFlag (key,new IntWritable (1));
            context.write (id,value);
        }
    }
    public static class GRRReducer extends Reducer<IdFlag,Text,Text,Text>{
        @Override
        protected void reduce(IdFlag key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //value
            Iterator<Text> ite=values.iterator ();
            //物品相似度
            Text gc=new Text (ite.next ());
            //某个物品被那些用户购买过
            Text up=new Text (ite.next ());
            //20001:10 20002:6
            String[] gcs=gc.toString ().split (",");
            //10001:1 10002:1
            String[] ups=up.toString ().split (",");
            for(String s:gcs){
                String[] item_gcs=s.split ("[:]");
                for (String u:ups){
                    String[] item_ups=u.split ("[:]");
                    String ids=item_gcs[0]+","+item_ups[0];
                    int rcr=Integer.parseInt (item_gcs[1])*Integer.parseInt (item_ups[1]);
                    context.write (new Text (ids),new Text (rcr+""));
                }
            }
        }
    }
}
