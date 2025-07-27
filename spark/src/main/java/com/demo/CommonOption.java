package com.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 通用API操作，适用于单元素以及KV元素
 *
 * @Author: 君墨笑
 * @Date: 2025/7/25 16:56
 */
public class CommonOption {

    public static void main(String[] args) {
        // 1. 创建 Spark 配置和上下文
        SparkConf conf = new SparkConf()
                .setAppName("Java WordCount")
                .setMaster("local[*]"); // 本地运行
        JavaSparkContext sc = new JavaSparkContext(conf);

        //fliter过滤
        /*JavaRDD<Integer> parallelize = sc.parallelize(Lists.newArrayList(1, 2, 3, 4, 5, 4, 3, 2, 1), 3);
        JavaRDD<Integer> filter = parallelize.filter(it -> it >= 3);
        filter.collect().forEach(System.out::println);*/

        //===========================================//
        //distinct去重
        /*JavaRDD<Integer> parallelize = sc.parallelize(Lists.newArrayList(1, 2, 2, 3, 4, 5, 4, 3, 2, 1), 3);
        JavaRDD<Integer> distinct = parallelize.distinct();
        distinct.collect().forEach(System.out::println);*/

        while (true) {
            
        }
    }
}