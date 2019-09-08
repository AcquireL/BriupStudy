package com.briup.common;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 *  工具类，帮助获得zk对象
 *  类似extends Config
 */
public class ConnectionWatcher implements Watcher {
    private String  address=null;
    private int timeout=3000;
    protected ZooKeeper zk=null;
    private CountDownLatch signal=new CountDownLatch (1);
    public ConnectionWatcher(String address) {
        this.address = address;
    }
    public void connect()throws Exception{
        zk=new ZooKeeper (address,timeout,this);
        signal.await ();
    }
    public void close() throws InterruptedException {
        zk.close ();
    }
    @Override
    public void process(WatchedEvent event) {
        if(event.getState ().compareTo (Event.KeeperState.SyncConnected)==0){
            System.out.println ("连接成功");
            signal.countDown ();
        }
    }
}
