package com.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class WordCountMain {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建 Spark 配置和上下文
        SparkConf conf = new SparkConf()
                .setAppName("Java WordCount")
                .setMaster("local[*]"); // 本地运行
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2. 读取文本文件（修改为你自己的路径）
        JavaRDD<String> lines = sc.textFile("/Users/pengshaoxiang/Documents/hadoop/hdfsLocalData/demo.txt");

        // 3. 按空格切分单词
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        // 4. 映射为 (word, 1)
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        // 5. 按 key 统计数量
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

        JavaPairRDD<Integer, Integer> reversePair = wordCounts.mapToPair(wc -> new Tuple2<>(wc._2, 1));

        JavaPairRDD<Integer, Integer> reverseCounts = reversePair.reduceByKey((v1, v2) -> v1 + v2);
        // 6. 输出结果
        reverseCounts.foreach(tuple -> System.out.println(tuple._1 + ": " + tuple._2));
        Thread.sleep(60000L);

        // 7. 关闭 Spark 上下文
        sc.close();
        System.out.println("Hello world!");
    }
}