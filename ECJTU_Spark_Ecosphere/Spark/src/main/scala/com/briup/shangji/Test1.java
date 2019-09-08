package com.briup.shangji;
/*
    写一个方法计算s=a+aa+aaa+aaaa+aaaaa,其中a取值范围是【1-9】，a作为这个方法得参数，s作为返回值
    例如：
        a=1，s=1
        a=2, s=2+22
        a=3, s=3+33+333
        a=4, s=4+44+444+4444
 */
public class Test1 {
    public static void main(String[] args) {
        System.out.println (fun(4));
    }
    public static int fun(int a){
        int s=a;
        int temp=a;
        for(int i=2;i<=a;i++){
            temp=temp+a*(int)Math.pow (10, i-1);
            s+=temp;
        }
        return s;
    }
}
