package com.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 适用于单元素的API练习
 *
 * @Author: 君墨笑
 * @Date: 2025/7/25 15:56
 */
public class KeyValueOption {

    public static void main(String[] args) {
        // 1. 创建 Spark 配置和上下文
        SparkConf conf = new SparkConf()
                .setAppName("Java WordCount")
                .setMaster("local[*]"); // 本地运行
        JavaSparkContext sc = new JavaSparkContext(conf);


        while (true) {

        }
    }
}