package com.briup.json;

public class HttpRequestTest {
    public static void main(String[] args) {
        String transJson ="[{\"briup\":\"100\",\"method\":\"query\",\"id\":5}]";
        System.out.println(transJson);
        //发送 POST 请求
        String sr=HttpRequest.sendPost("http://192.168.117.50:8888",transJson);
        System.out.println(sr);
    }
}
