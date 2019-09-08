package com.briup.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PutTest {
	Configuration conf = null;
	Connection conn = null;
	Admin admin = null;
	Table table = null;
	HTable t = null;
	ExecutorService pool;

	@Before
	public void before() throws Exception {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum",
				"client_lwj:2181");
		pool = Executors.newFixedThreadPool(10);
		conn = ConnectionFactory.createConnection(conf,pool);
		admin = conn.getAdmin();
	}
	@Test
	public void putTest1() throws IOException {
		table = conn.getTable(TableName.valueOf("bd1902:follower"));
		Put put = new Put(Bytes.toBytes("f5,s5"));
		put.addColumn(Bytes.toBytes("f"),
				Bytes.toBytes("to"),Bytes.toBytes("s5"));
		put.addColumn(Bytes.toBytes("f"),
				Bytes.toBytes("to_name"),Bytes.toBytes("chunchun"));
		put.addColumn(Bytes.toBytes("f"),
				Bytes.toBytes("from"),Bytes.toBytes("f5"));
		put.addColumn(Bytes.toBytes("f"),
				Bytes.toBytes("from_name"),Bytes.toBytes("zhangsan"));
		//--------------------------

		table.put(put);
	}
	@After
	public void after() throws IOException {
		conn.close();
	}
}
