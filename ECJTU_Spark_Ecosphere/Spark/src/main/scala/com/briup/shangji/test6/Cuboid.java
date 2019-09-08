package com.briup.shangji.test6;

public class Cuboid extends Rectangle{
    private double high;
    public Cuboid(double length, double wide,double high) {
        super (length, wide);
        this.high=high;
    }
    public void vol(){
        System.out.println (super.getLength ()*super.getWide ()*this.high);
    }

}
