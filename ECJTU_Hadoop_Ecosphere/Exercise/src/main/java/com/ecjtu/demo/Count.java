package com.ecjtu.demo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
/*
      统计字符串出现的次数
 */
public class Count {
    public static void main(String[] args) {
        String str="qqwr";
        char[] chars=str.toCharArray ();
        Map map=new HashMap<Character,Integer>();
        for(Character a:chars){
            if(!map.containsKey (a)) {
                map.put (a, 1);
            }else
                map.put (a,(Integer)map.get (a)+1 );
        }

        Set<Character> set=map.keySet ();
        for(Character s:set){
            System.out.println (s+" "+map.get (s));
        }

        for (Object entry:map.entrySet ()){
            System.out.println (entry.toString ());
        }

        Set<Map.Entry<Character,Integer>> set1=map.entrySet ();
        for(Map.Entry<Character,Integer> entry:set1){
            System.out.println (entry.getKey ()+" "+entry.getValue ());
        }

        Iterator<Map.Entry<Character,Integer>> it=map.entrySet ().iterator ();
        while(it.hasNext ()){
            System.out.println (it.next ());
        }
    }
}
/*
abstract class Abstract_Test{
    private int a;
    public Abstract_Test(int a){
        this.a=a;
    }
}*/
