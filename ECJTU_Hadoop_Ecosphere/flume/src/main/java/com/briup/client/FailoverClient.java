package com.briup.client;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.util.Properties;

public class FailoverClient {
	public static void main(String[] args) throws EventDeliveryException, InterruptedException {
		Properties props = new Properties();
		props.put("client.type", "default_failover");
		props.put("hosts", "h1 h2 h3");
		String host1 = "192.168.29.131:33333";
		String host2 = "192.168.29.131:44444";
		String host3 = "192.168.29.131:55555";
		props.put("hosts.h1", host1);
		props.put("hosts.h2", host2);
		props.put("hosts.h3", host3);

		RpcClient client =
			RpcClientFactory.getInstance(props);
		for (int i = 0; i < 10; i++) {
			Event event = EventBuilder.withBody
					(("helloworld "+i).getBytes());
			client.append(event);
			Thread.sleep(2000);
		}
		client.close();
	}
}
