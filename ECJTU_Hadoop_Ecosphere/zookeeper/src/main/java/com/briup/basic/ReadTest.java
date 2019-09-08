package com.briup.basic;

import com.briup.common.ConnectionWatcher;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class ReadTest extends ConnectionWatcher {
    private String path;
    public ReadTest(String address) {
        super (address);
    }

    @Override
    public void process(WatchedEvent event) {
        super.process (event);
        switch (event.getType ()){
            case NodeDataChanged:
                System.out.println (path+"该节点数据被改变");
                break;
            case NodeDeleted:
                System.out.println (path+"该节点被删除");
                break;
        }
        getData_asyn (path);
    }

    public void getData_syn(String path) throws KeeperException, InterruptedException {
        byte[] data = zk.getData (path, false, null);
        for (byte b : data) {
            char a = (char) b;
            System.out.print (a);
        }
    }
    public void getData_asyn(String path){
        this.path=path;
        AsyncCallback.DataCallback dc=new AsyncCallback.DataCallback(){

            @Override
            public void processResult(int rc,
                                      String path,
                                      Object ctx,
                                      byte[] data,
                                      Stat stat) {
                switch (KeeperException.Code.get (rc)){
                    case OK:
                        System.out.println ("读取的数据为:"+new String(data));
                        break;
                    case NONODE:
                        System.out.println ("读取的文件不存在！");
                        break;
                }
            }
        };
        zk.getData (path,this,dc,"Hello");
    }
    public void getChildren_syn(String path) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren (path, false);
        for(String childer:children){
            System.out.println (childer);
        }
    }
    public void getChildrn_asyn(String path){
        AsyncCallback.Children2Callback cc=new AsyncCallback.Children2Callback(){
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                switch (KeeperException.Code.get (rc)){
                    case OK:
                        System.out.println ("该节点的子节点为：");
                        for(String childer:children){
                            System.out.println (childer);
                        }
                        break;
                    case NONODE:
                        System.out.println ("该节点不存在！");
                        break;
                    case NOCHILDRENFOREPHEMERALS:
                        System.out.println ("该节点不存在子节点！");
                        break;
                }
            }
        };
            zk.getChildren (path,false, cc,"Hello");
        }

    public static void main(String[] args) throws Exception {
        ReadTest rt = new ReadTest ("192.168.29.130");
        rt.connect ();
        //rt.getData_syn ("/node");
//        rt.getData_asyn ("/node");
//        Thread.sleep (5000);
//
//        rt.getChildren_syn ("/node");
  //      rt.getChildrn_asyn ("/");
        rt.getData_asyn ("/node");
        Thread.sleep (500000);
        rt.close ();
    }
}
