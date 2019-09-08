package com.briup.shangji;

import java.util.Scanner;
//输入10个数，进行冒泡排序
public class Test4 {
    public static void main(String[] args) {
        Scanner sc=new Scanner (System.in);
        int []array=new int[10];
        for(int i=0;i<10;i++){
            array[i]=sc.nextInt ();
        }
        for(int i=0;i<array.length-1;i++){
            for(int j=0;j<array.length-i-1;j++){
                if(array[j]>array[j+1]){
                    int temp=array[j+1];
                    array[j+1]=array[j];
                    array[j]=temp;
                }
            }
        }
        for(int i=0;i<array.length;i++){
            System.out.print(array[i]+" ");
        }

    }
}
