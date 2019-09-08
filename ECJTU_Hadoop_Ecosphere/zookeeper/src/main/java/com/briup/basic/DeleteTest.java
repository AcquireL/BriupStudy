package com.briup.basic;

import com.briup.common.ConnectionWatcher;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

public class DeleteTest extends ConnectionWatcher {

    public DeleteTest(String address) {
        super (address);
    }
    public void delete_syn(String path) throws KeeperException, InterruptedException {
        zk.delete (path,-1);
    }
    public void delete_asyn(String paht){
        AsyncCallback.VoidCallback cb=new  AsyncCallback.VoidCallback(){
            @Override
            public void processResult(int rc,
                                      String path,
                                      Object ctx) {
                switch (Code.get (rc)){
                    case OK:
                        System.out.println ("删除成功！"+paht+" "+ctx);
                        break;
                    case NONODE:
                        System.out.println (paht+"该节点不存在。");
                        break;
                }
            }
        };
        zk.delete (paht,-1,cb,"hello");
    }
    public static void main(String[] args) throws Exception {
        DeleteTest dt=new DeleteTest ("192.168.29.131");
        dt.connect ();
        //dt.delete_syn ("/node1");
        dt.delete_asyn ("/node_syn");
        Thread.sleep (5000);
        dt.close ();
    }
}
