package com.briup.udf;


import org.apache.hadoop.hive.ql.exec.UDF;

//自定义hive的函数
//给定一个手机号，返回归属地
public class GetArea  extends UDF {

    public String evaluate(String phone){
        switch (phone.substring (0,3)){
            case "139":
                return "北京";
            case "138":
                return "上海";
            case "136":
                return "南昌";
                default:
                    return "未知";
        }
    }
}
