package com.briup.shangji.io;

import java.io.*;

public class InputStreamTest {
    public static void main (String[] args)throws Exception {
        //字节流转字符流
        InputStream is=new FileInputStream(new File ("Spark\\files\\info.txt"));
        InputStreamReader isr=new InputStreamReader (is);
     /* 读取中文
        //方法1
        byte[] arr=new byte[1024];
        int code;
        while((code=is.read (arr))!=-1){
            System.out.println (new String (arr,0,code));
        }*/
     //方法2
      /*  BufferedReader bf=new BufferedReader (isr);
        String line=null;
        while ((line=bf.readLine ())!=null){
            System.out.println (line);
        }*/
    }

}
