package com.briup.basic;

import com.briup.common.ConnectionWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

public class ExistsTest extends ConnectionWatcher {
    private String path;
    public ExistsTest(String address) {
        super (address);
    }

    @Override
    public void process(WatchedEvent event) {
        //需要这句话，保证zk连接成功
        super.process (event);
        switch (event.getType ()){
            case NodeCreated:
                System.out.println (event.getPath ()+"节点被创建");
                break;
            case NodeDeleted:
                System.out.println (event.getPath ()+"节点被删除");
                break;
            case NodeDataChanged:
                System.out.println (event.getPath ()+"节点数据发生改变");
                break;
            case NodeChildrenChanged:
                System.out.println (event.getPath ()+"节点子节点改变");
                break;
        }
        try {
            zk.exists (path,true);
        } catch (KeeperException e) {
            e.printStackTrace ();
        } catch (InterruptedException e) {
            e.printStackTrace ();
        }
    }
    public void setWatcher(String path) throws KeeperException, InterruptedException {
        this.path=path;
        zk.exists (path,this);
    }
    public static void main(String[] args) throws Exception {
        ExistsTest et=new ExistsTest ("192.168.29.130");
        et.connect ();
        et.setWatcher ("/node");
        Thread.sleep (300000);
        et.close ();
    }
}
