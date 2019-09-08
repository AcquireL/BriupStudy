package com.briup.coprocessor;

import com.google.protobuf.RpcCallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SumTest {
	Configuration conf = null;
	ExecutorService pool = null;
	Connection conn = null;
	Admin admin = null;
	Table table = null;
	HTable t = null;

	//获得hbase连接
	@Before
	public void before() throws IOException {
		Configuration conf = null;
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoopPD:2181");
		ExecutorService pool = null;
		pool = Executors.newFixedThreadPool(10);
		conn = ConnectionFactory.
				createConnection(conf, pool);
		admin = conn.getAdmin();
	}

	@Test
	public void SumClientTest() throws Throwable {
		table = conn.getTable
				(TableName.valueOf("bttc2:follower"));
		Sum.SumRequest request = Sum.SumRequest.newBuilder()
				.setFamily("1").setColumn("1").build();
		Map<byte[], Long> map =
				table.coprocessorService
						(Sum.SumService.class,
								null, null,
								new Batch.Call<Sum.SumService, Long>() {
									@Override
									public Long call(Sum.SumService sumService) throws IOException {
										RpcCallback<Sum.SumResponse> rcb =
												new BlockingRpcCallback<>();
										sumService.getSum(null, request, rcb);
										Sum.SumResponse sumResponse =
												((BlockingRpcCallback<Sum.SumResponse>) rcb).get();
										return sumResponse.getSum();
									}
								});
		for (Map.Entry<byte[], Long> e : map.entrySet()) {
			System.out.println("region_id:" + Bytes.toString(e.getKey())
					+ " 行数:" + e.getValue());
		}
	}

	@After
	public void after() throws IOException {
		conn.close();
	}
}
