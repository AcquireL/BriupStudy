package com.briup.zookeeper.lock;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;

import com.briup.common.ConnectionWatcher;

public class WriteLock extends ConnectionWatcher{
	//建立锁的节点
 	public static final String LOCK_NAME = "/lock";
 	//锁的编号
 	private long lockId;

	private CountDownLatch signal =
			new CountDownLatch(1);

	public WriteLock(String hosts){
		super(hosts);
	}
	 
	public long lock(String lock) throws Exception {
		connect();
		//建立锁节点
		String lockPath = 
				zk.create(LOCK_NAME + "/" + lock, 
				null, 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.EPHEMERAL_SEQUENTIAL);
		//取得锁节点编号
		lockId = Long.parseLong(
				lockPath.split("/")[2].trim());
		//如果是最小锁编号，则成功上锁
		if(lockId == getLeastId())return lockId;
		else{
			//在小一个编号的锁节点上设置观察
			zk.exists(LOCK_NAME + "/" + (lockId - 1), this);
			signal.await();
		}
		return lockId;
	}
 
	private long getLeastId() {
		try {
			List<String> nodes = 
					zk.getChildren(LOCK_NAME, false);
			SortedSet<Long>  st = new TreeSet<>();

			for(String p : nodes){
				st.add(Long.parseLong(p.trim()));
			}
			return st.first();
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
 		return 0;
	}

	@Override
	public void process(WatchedEvent event) {
 		super.process(event);
 		if(event.getType() == EventType.NodeDeleted){
 			if(lockId == getLeastId()){
 				//观察到自己持有最小锁节点，则释放信号。
 				signal.countDown();
 			}else{
 				try {
 					//继续在小一号锁节点上设置观察
					zk.exists(LOCK_NAME + "/" + (lockId - 1), this);
				} catch (KeeperException | InterruptedException e) {
 					e.printStackTrace();
				} 
 			}
 		}
	}

	public void unlock() throws InterruptedException{
		close();
	}
	public static void main(String... strings) throws KeeperException, InterruptedException {
		int j = 10;
		for (int i = 1; i < j; i++) {
			new Thread() {
				public void run() {
					WriteLock wl;
					try {
						wl = 
							new WriteLock("192.168.29.131:2181");
						long lock = wl.lock("1");
						System.out.println(
								Thread.currentThread().getName() + "lock : " + lock);
						Thread.sleep(3000);
						System.out.println(
								Thread.currentThread().getName() + " unlock : " + lock);
						wl.unlock();
 					}  catch (Exception e) {
 						e.printStackTrace();
					}
				}
			}.start();

		}
		Thread.sleep(90000);
	}
}
