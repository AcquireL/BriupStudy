package com.briup.shangji;

import javafx.scene.shape.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
/*
    一篇包含5000多个单词的文本文件，
    将里面的单词按其字母在26字母表里的位置获取一个位置（A为1，Z为26），再将每个字母的位置数相加，
    即得到单词得分，求出文件中得分最高得单词
 */
public class Test3 {
    public static void main(String[] args) throws FileNotFoundException {
        int max=0;
        String result=null;
        try {
            FileReader in=new FileReader (new File ("D:\\idea-hadoop-workpace\\ECJTU_Spark_Ecosphere\\Spark\\files\\ihaveadream.txt"));
            BufferedReader bf=new BufferedReader (in);
            String line=null;
            while(null!=(line=bf.readLine ())){
                String[] word = line.split ("[ ]");
               for(String s:word){
                   char[] low=s.toLowerCase ().toCharArray ();
                   for(char c:low){
                       int temp=c-'a'+1;
                       if(temp>max){
                           max=temp;
                           result=s;
                       }
                   }
               }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace ();
        } catch (IOException e) {
            e.printStackTrace ();
        }
        System.out.println (result);
    }
}
