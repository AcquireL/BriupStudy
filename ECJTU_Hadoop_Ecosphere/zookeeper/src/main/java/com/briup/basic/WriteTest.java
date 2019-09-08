package com.briup.basic;

import com.briup.common.ConnectionWatcher;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class WriteTest extends ConnectionWatcher {
    public WriteTest(String address) {
        super (address);
    }
    public void setData_syn(String path,String data) throws KeeperException, InterruptedException {
        zk.setData (path,data.getBytes (),-1);
        System.out.println ("同步修改成功！");
    }
    public void setData_asyn(String path,String data){
        AsyncCallback.StatCallback sb=new AsyncCallback.StatCallback () {
            @Override
            public void processResult(int rc, String path, Object o, Stat stat) {
                switch (KeeperException.Code.get (rc)){
                    case OK:
                        System.out.println ("异步修改成功,传入的对象为："+o);
                        break;
                }
            }
        };
        zk.setData (path,data.getBytes (),-1,sb,"hello");
    }
    public static void main(String[] args) throws Exception {
        WriteTest wt=new WriteTest ("192.168.29.131");
        wt.connect ();
        //wt.setData_syn ("/node","setData");
        wt.setData_asyn ("/node","setDeata_");
        Thread.sleep (5000);
        wt.close ();
    }
}
