package com.briup.basic;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

public class ZKConnection {
    private static CountDownLatch signal=new CountDownLatch (1);
    public static void main(String[] args)throws  Exception {
        //连接zk
        String hosts="192.168.29.133:2181";
        int timeout=1000;
        ZooKeeper zk = new ZooKeeper (hosts, timeout,
                event -> {
                    System.out.println (event.getType ());
                    System.out.println (event.getState ());
                    if(Watcher.Event.KeeperState.SyncConnected.compareTo (event.getState ())==0){
                        System.out.println ("连接成功!");
                        //让信号量的值 -1
                        signal.countDown ();
                    }
                });
        //逻辑操作
        //在主线程中写，注意让主线程等待
        //等待 信号变成 0
        signal.await ();
        System.out.println ("hello world");
        zk.close ();
    }
}
