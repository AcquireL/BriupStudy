package com.briup.grms.step8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DBStore extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DBStore(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		Job job=Job.getInstance(conf,"dbStore");
		job.setJarByClass(this.getClass());
		
		job.setMapperClass(DBMapper.class);
		job.setMapOutputKeyClass(UserGoodValue.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setReducerClass(Reducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(conf.get("inpath")));
		
		job.setOutputFormatClass(DBOutputFormat.class);
		DBConfiguration.configureDB(job.getConfiguration(),"com.mysql.jdbc.Driver" ,"jdbc:mysql://172.16.0.5:3306/grms","briup","briup");
		DBOutputFormat.setOutput(job, "hj_lwj_grms", "user_id","gid","res");
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static class DBMapper extends Mapper<LongWritable,Text,UserGoodValue,NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String line=value.toString();
			String[] info=line.split("[\t]");
			UserGoodValue ugv=new UserGoodValue(info[1],info[0],info[2]);
			context.write(ugv, NullWritable.get());
		}
	}

}
