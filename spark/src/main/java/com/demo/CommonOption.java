package com.demo;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Iterator;

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

        //创建一个初始数据集RDD，分为三个分区。
        //fliter过滤
        /*JavaRDD<Integer> parallelize = sc.parallelize(Lists.newArrayList(1, 2, 3, 4, 5, 4, 3, 2, 1), 3);
        JavaRDD<Integer> filter = parallelize.filter(it -> it >= 3);
        filter.collect().forEach(System.out::println);*/

        //distinct去重
        /*JavaRDD<Integer> parallelize = sc.parallelize(Lists.newArrayList(1, 2, 2, 3, 4, 5, 4, 3, 2, 1), 3);
        JavaRDD<Integer> distinct = parallelize.distinct();
        distinct.collect().forEach(System.out::println);*/

        //map 元素一对一转换操作
        /*JavaRDD<Integer> parallelize = sc.parallelize(Lists.newArrayList(1, 2, 3, 4, 5, 4, 3, 2, 1), 3);
        JavaRDD<Integer> map = parallelize.map(it -> it * 2);
        map.collect().forEach(System.out::println);*/

        //flatMap 元素一对多转换操作
        /*JavaRDD<String> parallelize = sc.parallelize(Lists.newArrayList("hello word","test unit","leo haha"), 3);
        JavaRDD<String> stringJavaRDD = parallelize.flatMap(line -> {
            String[] split = line.split(" ");
            return Lists.newArrayList(split).iterator();
        });
        stringJavaRDD.collect().forEach(System.out::println);*/

        //集合的并集、交集、差集、笛卡尔积
        /*JavaRDD<Integer> parallelize1 = sc.parallelize(Lists.newArrayList(1, 2, 3), 3);
        JavaRDD<Integer> parallelize2 = sc.parallelize(Lists.newArrayList(1, 2, 3,4,5), 3);*/
        /*JavaRDD<Integer> unionRDD = parallelize1.union(parallelize2);
        unionRDD.collect().forEach(System.out::println);*/
        /*JavaRDD<Integer> ntersectionRDD = parallelize1.intersection(parallelize2);
        ntersectionRDD.collect().forEach(System.out::println);*/
        /*JavaRDD<Integer> subtract = parallelize2.subtract(parallelize1);
        subtract.collect().forEach(System.out::println);*/
        /*JavaPairRDD<Integer, Integer> cartesian = parallelize1.cartesian(parallelize2);
        cartesian.collect().forEach(tuple -> System.out.println(tuple._1 + ":" + tuple._2));*/

        //分区操作
        /*JavaRDD<Integer> parallelize = sc.parallelize(Lists.newArrayList(1, 22, 3,10,4,8,2,9,11,13,15,14), 3);
        JavaRDD<String> stringJavaRDD = parallelize.mapPartitionsWithIndex((index, iterator) -> {
            return new Iterator<String>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }
                @Override
                public String next() {
                    if (!hasNext()) {
                        return null;
                    }
                    Integer next = iterator.next();
                    return index + "-" + next;

                }
            };
        },true);
        stringJavaRDD.collect().forEach(System.out::println);*/

        //排序操作
        /*JavaRDD<Integer> parallelize = sc.parallelize(Lists.newArrayList(1, 22, 3,10,4,8,2,9,11,13,15,14), 3);
        JavaRDD<Integer> sort = parallelize.sortBy(it -> it, true, 1);
        sort.collect().forEach(System.out::println);
        */

        while (true) {
            
        }
    }
}