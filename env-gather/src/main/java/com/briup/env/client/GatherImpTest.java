package com.briup.env.client;

import com.briup.env.bean.Environment;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;

public class GatherImpTest implements Gather {

    private String path="D:\\idea-hadoop-workpace\\env-gather\\src\\main\\resources\\data-file";
    @Override
    public Collection<Environment> gather() throws Exception {
        Collection<Environment> c=new ArrayList<>();
        FileInputStream in=new FileInputStream (path);
        BufferedReader bf=new BufferedReader (new InputStreamReader (in));
        String line=null;
        while((line=bf.readLine ())!=null){
            Environment env=new Environment ();
            String[ ] arr=line.split ("[|]");
            env.setSrcId (arr[0]);
            env.setDesId (arr[1]);
            env.setDevId (arr[2]);
            env.setSersorAddress (arr[3]);
            env.setCount (Integer.parseInt (arr[4]));
            env.setCmd (arr[5]);
            env.setStatus (Integer.parseInt (arr[7]));
            env.setGather_date (new Timestamp (Long.parseLong (arr[8])));
            if("16".equals (arr[3])){
                int data=Integer.parseInt (arr[6].substring (0,4),16);
                env.setName ("温度");
                env.setData ((float)(data*0.00268127-46.85));
                c.add (env);
                env=new Environment ();
                env.setSrcId (arr[0]);
                env.setDesId (arr[1]);
                env.setDevId (arr[2]);
                env.setSersorAddress (arr[3]);
                env.setCount (Integer.parseInt (arr[4]));
                env.setCmd (arr[5]);
                env.setStatus (Integer.parseInt (arr[7]));
                env.setGather_date (new Timestamp (Long.parseLong (arr[8])));
                env.setName ("湿度");
                int dataSiDu=Integer.parseInt (arr[6].substring (4,8),16);
                env.setData ((float)(dataSiDu*0.00190735-6));
                c.add (env);
            }else{
                float data=Integer.parseInt (arr[6].substring (0,4),16);
                if("256".equals (arr[3])){
                    env.setName ("光照强度");
                    env.setData (data);
                    c.add (env);
                }
                if("1280".equals (arr[3])){
                    env.setName ("二氧化碳");
                    env.setData (data);
                    c.add (env);
                }
            }
        }
        return c;
    }
    public static void main(String[] args)throws Exception{
        Gather g = new GatherImpTest ();
        Collection<Environment> c = g.gather ();
        System.out.println (c.size ());
        for (Environment e:c){
            System.out.println (e.toString ());
        }
    }
}
