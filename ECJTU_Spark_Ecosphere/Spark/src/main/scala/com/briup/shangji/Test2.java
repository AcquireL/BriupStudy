package com.briup.shangji;

public class Test2 {
    public static void main(String[] args) {
        int a[][]={{3,2,6},{6,8,2,10},{5},{12,3,23}};
        int max=a[0][0],min=a[0][0];
        for(int i=0;i<a.length;i++){
            for(int j=0;j<a[i].length;j++){
                if(max<a[i][j]){
                    max=a[i][j];
                }
                if(min>a[i][j]){
                    min=a[i][j];
                }
            }
        }
        System.out.println ("最大值："+max+" 最小值："+min);
    }
}
