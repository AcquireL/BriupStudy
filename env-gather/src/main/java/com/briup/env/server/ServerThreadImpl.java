package com.briup.env.server;

import com.briup.env.bean.Environment;
import sun.security.pkcs11.Secmod;

import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ServerThreadImpl implements Server{
    private int port=8888;
    private ExecutorService pool;
    private int poolSize=5;
    public ServerThreadImpl() {
        //获取一个固定长度的线程池
        pool= Executors.newFixedThreadPool(poolSize);
    }
    @Override
    public void reciver() throws Exception {
        ServerSocket ss =new ServerSocket (port);

        while (true){
            Socket socket=ss.accept ();
            System.out.println ("服务器与ip为"+socket.getInetAddress ().getHostName ()+"的客服端建立了连接");
            //new Thread (new Handler (socket)).start ();
            pool.execute (new Handler (socket));
        }
    }
    private class Handler implements Runnable {
        private Socket socket;
        private ObjectInputStream in;

        public Handler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
          try{
              in=new ObjectInputStream (socket.getInputStream ());
              Collection<Environment> c=(Collection<Environment>)in.readObject ();
              System.out.println ("服务器接收到客服端"+c.size ()+"条数据");

              //调用DBstore,执行入库操作
              DBStore dbStore=new DBStoreImpl ();
              long start=System.currentTimeMillis ();
              dbStore.saveDB (c);
              long end=System.currentTimeMillis ();
              System.out.println ("入库完成，共"+c.size()+"条，用时"+(end-start)+"毫秒");

          }catch (Exception e){
              e.printStackTrace ();
          }finally{
              try {
                  if(in!=null) in.close ();
                  if (socket!=null) socket.close ();
              }catch (Exception e){
                  e.printStackTrace ();
              }
            }
        }
    }

    @Override
    public void shutdown() throws Exception {

    }

}
