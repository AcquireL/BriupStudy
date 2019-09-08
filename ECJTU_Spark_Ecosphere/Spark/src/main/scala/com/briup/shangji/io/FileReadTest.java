package com.briup.shangji.io;

import java.io.*;
/*
    字节输入输出
 */
public class FileReadTest {
    public static void main(String[] args) throws Exception {
        FileReader in=new FileReader (new File ("Spark\\files\\ihaveadream.txt"));
        FileWriter out=new FileWriter (new File ("Spark\\files\\test.txt"));

        BufferedReader bf=new BufferedReader (in);
        String line=null;
        while(null!=(line=bf.readLine ())){
           // out.write (line+"\n");
        }
    }
}
