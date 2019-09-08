package com.briup.client;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;

public class AvroClient {
	public static void main(String[] args) throws EventDeliveryException, InterruptedException {
			RpcClient client = RpcClientFactory.
				getDefaultInstance
					("192.168.29.131", 5140);
			Event event = EventBuilder.withBody
					(("hello ").getBytes());
			client.append(event);

			Thread.sleep(2000);
			client.close();

	}
}
