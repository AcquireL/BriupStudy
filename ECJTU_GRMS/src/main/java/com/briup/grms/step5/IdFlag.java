package com.briup.grms.step5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用来做二次排序的复合键
 */
public class IdFlag implements WritableComparable<IdFlag> {
    //商品id连接键
    private Text gid=new Text ();
    //标识来自某个文件
    private IntWritable flag=new IntWritable ();
    //排序
    public IdFlag(){

    }

    public IdFlag(Text gid, IntWritable flag) {
        this.gid = new Text (gid.toString ());
        this.flag = new IntWritable (flag.get ());
    }

    public Text getGid() {
        return gid;
    }

    @Override
    public int compareTo(IdFlag o) {
        return this.gid.compareTo (o.gid)==0?this.flag.compareTo (o.flag):this.gid.compareTo (o.gid);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        gid.write (out);
        flag.write (out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        gid.readFields (in);
        flag.readFields (in);
    }

}
