package com.demo.utils;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class NginxMain {

    private static final List<String> nameList = Lists.newArrayList("zhangsan","lisi","wangwu","zhaoliu","nihao","xueying","wwanglin","lindong");
    private static final List<String> platList = Lists.newArrayList("fox","google","apple");


    public static void main(String[] a) throws IOException {
        String url = "http://localhost:8080/sendLog?name=%s&plat=%s&time=%s";
        System.out.println("开始执行");
        for (int i=0; i<10000; i++) {
            int random = ThreadLocalRandom.current().nextInt(0, 8);
            int random2 = ThreadLocalRandom.current().nextInt(0, 3);
            String time = getRandomDateInJune2025();
            OkHttpUtil.get(String.format(url,nameList.get(random),platList.get(random2),time),null);
        }
        System.out.println("结束执行");
    }

    public static String getRandomDateInJune2025() {
        Random random = new Random();
        int day = random.nextInt(30) + 1; // 1 到 30

        LocalDate date = LocalDate.of(2025, 6, day);
        return date.format(DateTimeFormatter.ISO_LOCAL_DATE);
    }
}
