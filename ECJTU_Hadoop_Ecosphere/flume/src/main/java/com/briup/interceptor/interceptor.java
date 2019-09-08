package com.briup.interceptor;

import java.util.Date;

public class interceptor {
    public static void main(String[] args) throws InterruptedException {
        while (true) {
            System.out.println((new Date ().getTime()));
            System.out.println("{\"requestTime\":"
                    + System.currentTimeMillis()
                    + ",\"requestParams\":{\"timestamp\":1405499314238,\"phone\":\"051255667690\",\"cardName\":\"TestName\",\"provinceCode\":\"201306\",\"cityCode\":\"456780\"},\"requestUrl\":\"/reporter-api/reporter/reporter12/init.do\"}");
            Thread.sleep(2000);

        }
    }
}
