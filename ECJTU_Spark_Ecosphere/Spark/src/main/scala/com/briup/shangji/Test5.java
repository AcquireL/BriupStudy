package com.briup.shangji;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

//随机生成（1-30中间）生成7个随机数，注意不能重复
public class Test5 {
    public static void main(String[] args) {
        Random rd=new Random ();
        Set set=new HashSet<Integer> ();
        int flag=0;
        while(set.size ()==7?false:true){
            int temp=rd.nextInt (30)+1;
            set.add (temp);
        }
        if(set.contains (7)){
            System.out.println ("中一等奖");
            flag=1;
        }
       if(set.contains (6)){
            System.out.println ("中二等奖");
            flag=1;
        }
        if(set.contains (5)){
            System.out.println ("中三等奖");
            flag=1;
        }
        if(flag==0){
            System.out.println ("未中奖");
        }

        for(Object num:set){
            System.out.println ((Integer) num);
        }
    }
}
