package com.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

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

        JavaPairRDD<String, Integer> javaPairRDD1 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("zhangsan", 18),
                new Tuple2<>("lisi", 20), new Tuple2<>("wangwu", 40),new Tuple2<>("sunqi", 82)));
        JavaPairRDD<String, Integer> javaPairRDD2 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("zhangsan", 24),
                new Tuple2<>("lisi", 33), new Tuple2<>("wangwu", 16),new Tuple2<>("zhaoliu", 22)));
        JavaPairRDD<String, Integer> javaPairRDD3 = sc.parallelizePairs(Arrays.asList(new Tuple2<>("zhangsan", 24),
                new Tuple2<>("zhangsan", 33), new Tuple2<>("wangwu", 16),new Tuple2<>("wangwu", 22)));
        //reduceByKey
        /*JavaPairRDD<String, Integer> reduceByKey = javaPairRDD3.reduceByKey((v1, v2) -> v1 + v2);
        reduceByKey.foreach(tuple -> System.out.println(tuple._1 + ": " + tuple._2));*/

        //groupByKey
        /*JavaPairRDD<String, Iterable<Integer>> groupedByKey = javaPairRDD3.groupByKey();
        groupedByKey.collect().forEach(it -> {
            System.out.println(it._1());
            it._2().forEach(System.out::println);
        });*/

        //join内连接
        /*JavaPairRDD<String, Tuple2<Integer, Integer>> join = javaPairRDD1.join(javaPairRDD2);
        join.collect().forEach(tuple -> System.out.println(tuple._1 + ": " + tuple._2._1 + "-" + tuple._2._2));*/
        //左连接
        /*JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> leftJoin = javaPairRDD1.leftOuterJoin(javaPairRDD2);
        leftJoin.collect().forEach(tuple -> System.out.println(tuple._1 + ": " + tuple._2._1 + "-" + tuple._2._2.orElse(0)));*/
        //右连接
        /*JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> rightJoin = javaPairRDD1.rightOuterJoin(javaPairRDD2);
        rightJoin.collect().forEach(tuple -> System.out.println(tuple._1 + ": " + tuple._2._1.orElse(0) + "-" + tuple._2._2));*/
        //cogroup操作
        /*JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroup = javaPairRDD1.cogroup(javaPairRDD2);
        cogroup.collect().forEach(tuple -> {
            StringBuilder builder = new StringBuilder();
            builder.append(tuple._1);
            builder.append(":");
            Iterator<Integer> iterator1 = tuple._2._1.iterator();
            Iterator<Integer> iterator2 = tuple._2._2.iterator();
            if (iterator1.hasNext()) {
                builder.append(iterator1.next());
            } else {
                builder.append("null");
            }
            builder.append("-");
            if (iterator2.hasNext()) {
                builder.append(iterator2.next());
            } else {
                builder.append("null");
            }
            System.out.println(builder.toString());
        });*/

        //combineByKey操作

        while (true) {

        }
    }
}