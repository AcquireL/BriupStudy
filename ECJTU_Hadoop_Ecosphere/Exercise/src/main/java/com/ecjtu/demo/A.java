package com.ecjtu.demo;

public class A {
    public A() {
        System.out.println ("this is A");
    }
    static {
        System.out.println ("this is A static code");
    }
    public static void main(String[] args) {
        new B ();
    }
}
class B extends A {
    public B() {
        System.out.println ("this is B");
        new A ();
    }
}