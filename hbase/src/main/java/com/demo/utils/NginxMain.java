package com.demo.utils;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class NginxMain {

    private static final List<String> nameList = Lists.newArrayList("zhangsan","lisi","wangwu","zhaoliu","nihao","xueying","wwanglin","lindong");


    public static void main(String[] a) throws IOException {
        String url = "http://localhost:8080/sendLog?name=%s";
        System.out.println("开始执行");
        for (int i=0; i<10000; i++) {
            int random = ThreadLocalRandom.current().nextInt(0, 8);
            OkHttpUtil.get(String.format(url,nameList.get(random)),null);
        }
        System.out.println("结束执行");
    }
}
