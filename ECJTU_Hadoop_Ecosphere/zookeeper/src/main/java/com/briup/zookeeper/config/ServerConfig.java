package com.briup.zookeeper.config;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import org.apache.zookeeper.KeeperException;

public class ServerConfig {
	public static final String 
	HOST_PORT_CONF="/serverConfig";
	
	private ActiveKeyValueStore store;
	private Random random = new Random();

	public ServerConfig(ActiveKeyValueStore store) throws IOException, InterruptedException{
 		this.store = store;
 	}
	
	//获取本机ip，生成随机的port填入到zookeeper服务器的/serverConfig节点中
	public void register(){
		int port = random.nextInt(65535);
		if(port < 1024) port += 1024;
		try {
			String host = 
					Inet4Address.getLocalHost()
						.getHostAddress();
			host = host + ":" + port;
			System.out.println (host);
			//写如zk服务器
			store.write(HOST_PORT_CONF, host);
 		} catch (Exception e) {
 			e.printStackTrace();
		} 
	}

	public String read(String hostPortConf, Executor executor) throws InterruptedException, KeeperException {
		return store.read(ServerConfig.HOST_PORT_CONF);
	}
	
 
}
