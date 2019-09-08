package com.ecjtu.demo;

import java.io.*;
import java.net.Socket;


public class Client {
    public static void main(String[] args) throws IOException {
        Socket socket=new Socket ("127.0.0.1",8888);
        InputStream is=new DataInputStream (socket.getInputStream ());
        OutputStream os=new FileOutputStream ("D:\\idea-hadoop-workpace\\ECJTU_Hadoop_Ecosphere\\Exercise\\src\\main\\resources\\"+System.currentTimeMillis ()+".jpg");
//        byte[ ] data=new byte[1024];
        int len=0;
        while ((len=is.read ())!=-1){
            os.write (len);
        }
        os.flush ();
        os.close ();
        is.close ();
        socket.close ();
    }
}
