package com.briup.zookeeper.config;

import com.briup.common.ConnectionWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ActiveKeyValueStore 
extends ConnectionWatcher {
	public ActiveKeyValueStore(String hostPort) {
		super(hostPort);
 	}

	public void write(String path, String value) throws InterruptedException, KeeperException {
		//检查zookeeper服务器上是否有path节点
		Stat stat = zk.exists(path, false);
		//path节点不存在则新建为临时节点，并设置节点内容
		if (stat == null) {
			zk.create(path, value.getBytes(), 
					Ids.OPEN_ACL_UNSAFE, 
					CreateMode.EPHEMERAL);
		} else {
			//如果path节点存在，则重设节点内容
			zk.setData(path, value.getBytes(), -1);
		}
	}

	public String read(String path) throws InterruptedException, KeeperException {
		//读取path节点内容并返回
		byte[] data = zk.getData(path, false, null);
		return new String(data);
	}
}
