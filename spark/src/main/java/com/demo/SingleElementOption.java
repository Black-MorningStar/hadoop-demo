package com.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 适用于单元素的API练习
 *
 * @Author: 君墨笑
 * @Date: 2025/7/25 15:56
 */
public class SingleElementOption {

    public static void main(String[] args) {
        // 1. 创建 Spark 配置和上下文
        SparkConf conf = new SparkConf()
                .setAppName("Java WordCount")
                .setMaster("local[*]"); // 本地运行
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建一个初始数据集RDD，分为三个分区。 map 元素一对一转换操作
        /*JavaRDD<Integer> parallelize = sc.parallelize(Lists.newArrayList(1, 2, 3, 4, 5, 4, 3, 2, 1), 3);
        JavaRDD<Integer> map = parallelize.map(it -> it * 2);
        map.collect().forEach(System.out::println);*/

        //===========================================//
        //flatMap 元素一对多转换操作
        /*JavaRDD<String> parallelize = sc.parallelize(Lists.newArrayList("hello word","test unit","leo haha"), 3);
        JavaRDD<String> stringJavaRDD = parallelize.flatMap(line -> {
            String[] split = line.split(" ");
            return Lists.newArrayList(split).iterator();
        });
        stringJavaRDD.collect().forEach(System.out::println);*/

        //===========================================//

        while (true) {

        }
    }
}