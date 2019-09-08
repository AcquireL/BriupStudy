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
import java.util.*;

public class PageRank extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run (new PageRank (), args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf ();
        //conf.set ("hbase.zookeeper.quorum", "master:2181,slave:2181");
        conf.set ("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
        Job job=Job.getInstance (conf, "lwj_pageRank");
        job.setJarByClass (this.getClass ());
        TableMapReduceUtil.initTableMapperJob("lwj:CleanDataMR",
                new Scan (),RankMapper.class,ImmutableBytesWritable.class,Text.class,job);
        TableMapReduceUtil.initTableReducerJob("lwj:CleanDataMR",RankReducer.class,job);
        job.waitForCompletion(true);

        for (int i = 1; i < 10; i++) {
            Configuration conf_ite = getConf();
            Job job_ite = Job.getInstance(conf_ite,"PageRank"+i);
            job_ite.setJarByClass(PageRank.class);
            TableMapReduceUtil.initTableMapperJob("lwj:CleanDataMR",
                    new Scan(),RankMapper.class,ImmutableBytesWritable.class,Text.class,job_ite);
            TableMapReduceUtil.initTableReducerJob("lwj:CleanDataMR",RankReducer.class,job_ite);
            job_ite.setNumReduceTasks(1);
            job_ite.waitForCompletion(true);
        }
        return 0;
    }

    public static class RankMapper extends TableMapper<ImmutableBytesWritable, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] url = value.getValue (Bytes.toBytes ("useinfo"), Bytes.toBytes ("url"));
            String base_url=Bytes.toString (url);
            if(base_url!=null){
                //获取出连接数
                byte[] num = value.getValue(Bytes.toBytes("useinfo"), Bytes.toBytes("oln"));
                int i = 0;
                if (num != null) {
                    i = Bytes.toInt(num);
                }
                // 设置每个网页初始权重值,或初始值为10
                byte[] rank_byte = value.getValue(Bytes.toBytes("useinfo"), Bytes.toBytes("rank"));
                //当前页面权重值
                double rank = 10;
                if (rank_byte != null) {
                    rank = Bytes.toDouble(rank_byte);
                }
                if (i > 0) {
                    // 拿到所有外链接
                    NavigableMap<byte[], byte[]> ols = value.getFamilyMap(Bytes.toBytes("ol"));
                    // 获得ols中所有的外链，即key值
                    Set<byte[]> olset = ols.keySet();
                    // 计算每个外链得到的权重
                    double score = rank / (double) i;
                    // 每个外链携带自己的分数进行输出
                    for (byte[] ol : olset) {
                        context.write (new ImmutableBytesWritable(ol), new Text(score+""));
                    }
                } else {
                    // 对于没有外链的页面，即对于所有网页贡献皆为0
                    context.write(new ImmutableBytesWritable (url), new Text(0 + ""));
                }
            }
        }
    }
    public static class RankReducer extends TableReducer<ImmutableBytesWritable,Text, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double factor = 0.85d;
            double sum = 0.0;
            String rowKey=Bytes.toString (key.get ()).trim ();
            String uriKey=returnNutchKey (rowKey);
            Put put=new Put (Bytes.toBytes (uriKey));
            for(Text value:values){
                double v = Double.parseDouble(value.toString ().trim ());
                sum += v;
            }
            double rank = sum*factor+(1-factor);
            put.addColumn(Bytes.toBytes("useinfo"), Bytes.toBytes("rank"),Bytes.toBytes(rank));
            context.write(NullWritable.get(),put);
        }
        public static String returnNutchKey(String key){
            // com.aliyun.bbs:http/
            // https://bbs.aliyun.com/thread/356.html
            String[] splits = key.split("://");
            String protocol = splits[0];
            String d = splits[1].substring(0,splits[1].indexOf("/"));
            String res = splits[1].substring(splits[1].indexOf("/"),splits[1].length());
            String[] domain = d.split("\\.");
            List<String> strings = Arrays.asList(domain);
            Collections.reverse(strings);
            String rs = "";
            for (String info: strings) {
                rs += info+".";
            }
            rs = rs.substring(0,rs.length()-1);
            return rs.trim()+":"+protocol.trim()+res.trim();
        }
    }
}
