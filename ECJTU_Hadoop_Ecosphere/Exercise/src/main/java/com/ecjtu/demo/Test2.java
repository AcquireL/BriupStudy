package com.ecjtu.demo;

public class Test2 {
    String[] strings;

    public static void main(String[] args) {
        Test2 a = new Test2 ();
        System.out.println (a.test ());
    }
    public int test() {
        int a = 0;
        try {
            a = 10;
            System.out.println (strings.toString ());
            return a;
        } catch (Exception e) {
            a = 20;
           return  a;
        } finally {
            a = 30;
            return a;
        }
    }

}
