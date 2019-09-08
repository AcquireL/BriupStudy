package com.ecjtu.demo;


import java.io.*;
import java.util.ArrayList;
import java.util.List;

/*
    统计java平均成
 */
public class GradeAvg {
    public static void main(String[] args) throws IOException {
        function ();
    }
    public static void function() throws IOException {
      /*  InputStream is=new FileInputStream ("D:\\idea-hadoop-workpace\\ECJTU_Hadoop_Ecosphere\\Exercise\\src\\main\\resources\\grade");
        int a=0;
        while((a=is.read ())!=-1){
            System.out.print ((char)a);
        }*/
      Reader reader=new FileReader ("D:\\idea-hadoop-workpace\\ECJTU_Hadoop_Ecosphere\\Exercise\\src\\main\\resources\\grade");
      BufferedReader br=new BufferedReader (reader);
        String line=null;
        int sum=0;
        int count=0;
        while ((line=br.readLine ())!=null){
            String[] info = line.split ("[-]");
            if(info[1].equals ("java")){
                sum+=Integer.parseInt (info[2]);
                count++;
            }
      }
        System.out.println (sum/count);
    }
}
