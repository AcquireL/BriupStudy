package com.ecjtu.demo;

import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    public static void main(String[] args) throws Exception {
        ServerSocket ss=new ServerSocket (8888);
        while(true){
            Socket socket=ss.accept ();
            String client_ip=socket.getInetAddress ().getHostName ()+"  "+socket.getPort ();
            System.out.println (client_ip);
            OutputStream os=new DataOutputStream (socket.getOutputStream ()) ;
            InputStream piciure= new FileInputStream ("D:\\idea-hadoop-workpace\\ECJTU_Hadoop_Ecosphere\\Exercise\\src\\main\\resources\\壁纸.jpg");
            byte[] data=new byte[1024];
            int len;
            while ((len=piciure.read (data))!=-1){
                os.write (data);
            }
            os.flush ();
            os.close ();
            piciure.close ();
            socket.close ();
        }
    }
}
