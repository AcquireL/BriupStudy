package com.briup.grms.step8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class UserGoodValue implements DBWritable,WritableComparable<UserGoodValue>{
	private Text user_id=new Text();
	private Text gid=new Text();
	private IntWritable res=new IntWritable();
	public UserGoodValue() {
		
	}
	public UserGoodValue(String user,String goods,String value) {
		this.user_id=new Text(user.toString());
		this.gid=new Text(goods.toString());
		this.res=new IntWritable(Integer.parseInt(value));
	}
	@Override
	public void write(DataOutput out) throws IOException {
		user_id.write(out);
		gid.write(out);
		res.write(out);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		user_id.readFields(in);
		gid.readFields(in);
		res.readFields(in);
	}
	
	@Override
	public void write(PreparedStatement prep) throws SQLException {
		prep.setString(1, this.user_id.toString());
		prep.setString(2, this.gid.toString());
		prep.setInt(3, this.res.get());
	}
	@Override
	public void readFields(ResultSet res) throws SQLException {
		if(res==null) {
			return;
		}
		this.user_id=new Text(res.getString(1));
		this.gid=new Text(res.getString(2));
		this.res=new IntWritable(res.getInt(3));
	}
	@Override
	public int compareTo(UserGoodValue o) {
		return this.user_id.compareTo(o.user_id)==0?this.gid.compareTo(o.gid):this.user_id.compareTo(o.user_id);
	}
	

}
