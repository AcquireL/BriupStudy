package com.briup.shangji.test6;

public class Rectangle {
    private double length;
    private double wide;
    public Rectangle(double length,double wide){
        this.length=length;
        this.wide=wide;
    }
    public double getLength() {
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }

    public double getWide() {
        return wide;
    }

    public void setWide(double wide) {
        this.wide = wide;
    }
    public void area(){
        System.out.println (this.length*this.wide);
    }
}
