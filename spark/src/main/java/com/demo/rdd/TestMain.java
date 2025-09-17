package com.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Author: 君墨笑
 * @Date: 2025/8/17 14:06
 */
public class TestMain {

    public static void main(String[] a) {
        // 1. 创建 Spark 配置和上下文
        SparkConf conf = new SparkConf()
                .setAppName("Java WordCount")
                .setMaster("local[*]"); // 本地运行
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<>("zhangsan", 18),
                new Tuple2<>("lisi", 20), new Tuple2<>("wangwu", 40),new Tuple2<>("sunqi", 82),new Tuple2<>("sunqi", 33)),3);
        JavaPairRDD<String, Integer> mapRDD = javaPairRDD.mapValues(value -> value + 1);
        JavaPairRDD<String, Integer> reduceRDD = mapRDD.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<String, Integer> finalRDD = reduceRDD.mapValues(value -> value + 1);
        finalRDD.collect().forEach(tuple -> System.out.println(tuple._1 + ": " + tuple._2));
        while (true) {

        }
    }
}