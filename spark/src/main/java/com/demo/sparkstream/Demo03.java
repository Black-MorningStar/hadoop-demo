package com.demo.sparkstream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 演示作用域在Executor端的变量
 *
 * @Author: 君墨笑
 * @Date: 2025/9/19 14:18
 */
public class Demo03 {

    public static void main(String[] args) throws InterruptedException {
        // 创建SparkConf对象
        SparkConf sparkConf = new SparkConf().setAppName("dataStream").setMaster("local[*]");
        //Spark Straming是微批处理,每5秒触发一次，将这5秒的数据划分成一个微批次，进行处理计算
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        streamingContext.sparkContext().setLogLevel("ERROR");

        //Driver端  从Socket端口读取数据源，每5秒一个批次进行处理。Driver的批次处理是串行单线程的，上一个批次没有处理完毕，下一个批次会等待着
        JavaReceiverInputDStream<String> dataStream = streamingContext.socketTextStream("localhost", 9666);
        //每5秒获取到一个批次后，Driver端会将该批次数据集打包成DStream对象，然后对该DStream数据集进行计算处理。
        //DataStream底层还是一个RDD，每个微批的数据其实底层是打包成一个RDD，遵循惰性执行，因此下面的这段代码只是定义计算逻辑，还是在Driver端运行的。
        JavaDStream<String> flatMapDs = dataStream
                .flatMap(value -> Arrays.stream(value.split(" ")).iterator());

        //由于DataStream底层还是RDD，所以可以通过transform函数将DataStream转换成对应的RDD，进行更多的算子调用
        //transform函数转换成RDD后，一般是对RDD操作计算算子,比如map、fliter等等,不会进行action算子操作，transform函数返回结果必须是一个RDD
        JavaPairDStream<String, Integer> wordCount = flatMapDs.transformToPair(rdd -> rdd.mapToPair(value -> new Tuple2<>(value, 1))
                .reduceByKey((v1, v2) -> v1 + v2));

        //这里定义的变量是在Driver端进程里定义的
        AtomicInteger count = new AtomicInteger(0);

        //如果要对RDD进行action动作算子操作,需要使用foreachRDD函数。
        // 这里的foreachRDD方法也是在Driver端运行的，只是将DataStream对象转换成RDD对象
        wordCount.foreachRDD(rdd -> {
            //如果是下面这种对count的操作，则是在Executor端执行的。
            JavaRDD<Tuple2<String, Integer>> map = rdd.map(value -> {
                //这里对于count变量的操作是在RDD的闭包函数内操作的，RDD相关的闭包函数会在Executor端进行计算，
                // 因此这里的count变量会在Drvier端下发给Executor的端时候copy打包一份发给Executor端。因此对于Driver端的count变量是不会有影响的
                int executorCount = count.incrementAndGet();
                System.out.println("======打印Executor端计数：" + executorCount + " =========");
                return value;
            });
            List<Tuple2<String, Integer>> collect = map.collect();
            collect.forEach(tuple -> System.out.println(tuple._1 + ": " + tuple._2));
        });

        //启动程序开始微批流处理
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}