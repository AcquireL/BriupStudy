package com.briup.basic;

import com.briup.common.ConnectionWatcher;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.Code;

public class CreateTest extends ConnectionWatcher {
    public CreateTest(String address) {
        super (address);
    }
    public void create_syn(String path,String data) throws KeeperException, InterruptedException {
        String name = zk.create (path, data.getBytes (), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println ("同步方式创建节点："+name);
    }
    public void create_asyn(String path,String data){
        //监听器
        AsyncCallback.StringCallback sc=new AsyncCallback.StringCallback () {
            @Override
            //rc 状态码
            //path 创建Znode的路径,带序号
            //ctx //从回调对象外部传入的对象
            //name //创建Znode的路径，不带序号
            public void processResult(int rc, String path, Object ctx, String name) {
                switch (Code.get (rc)){
                    case OK:
                        System.out.println ("异步创建成功："+name+" "+path);
                        System.out.println ("从外部传入的对象:"+ctx);
                        break;
                    case NODEEXISTS:
                        System.out.println ("节点已经存在");
                        break;
                }
            }
        };
        zk.create (path, data.getBytes (), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,sc,"hello");
    }
    public static void main(String[] args) throws Exception {
        CreateTest ct=new CreateTest ("192.168.29.131");
        ct.connect ();
        ct.create_syn ("/node_syn","hello");
        System.out.println ("==================");
        ct.create_asyn ("/node_asyn","world");
        Thread.sleep (5000);
        ct.close ();
    }
}
