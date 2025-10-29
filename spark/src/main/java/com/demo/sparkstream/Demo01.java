package com.demo.sparkstream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 演示DataStream 基本API操作
 *
 * @Author: 君墨笑
 * @Date: 2025/9/17 19:21
 */
public class Demo01 {

    public static void main(String[] args) throws InterruptedException {
        // 创建SparkConf对象
        SparkConf sparkConf = new SparkConf().setAppName("dataStream").setMaster("local[*]");
        //Spark Straming是微批处理,设置每5秒触发一次，将这5秒的数据划分成一个微批次，进行处理计算。批次就是最小的处理粒度了，不可再分。
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        streamingContext.sparkContext().setLogLevel("ERROR");

        //Driver端  从Socket端口读取数据源，每5秒一个批次进行处理。Driver的批次处理是串行单线程的，上一个批次没有处理完毕，下一个批次会等待着
        JavaReceiverInputDStream<String> dataStream = streamingContext.socketTextStream("localhost", 9666);

        //每5秒获取到一个批次后，Driver端会将该批次数据打包成一个RDD并包装成DStream对象。
        //DataStream底层还是RDD，遵循惰性执行，因此下面的这段代码只是定义计算逻辑，还是在Driver端运行的。
        JavaPairDStream<String, Integer> wordCount = dataStream
                .flatMap(value -> Arrays.stream(value.split(" ")).iterator())
                .mapToPair(value -> new Tuple2<>(value, 1))
                .reduceByKey((v1, v2) -> v1 + v2);
        //这里的print方法是Action算子，会触发底层RDD真正生成一个Job，并开始走DAG分析划分成多个Stage，每个Stage拆分成多个Task，并提交给Executor执行。
        wordCount.print();

        //启动程序开始微批流处理
        streamingContext.start();
        //阻塞等待程序结束,防止JVM进程退出
        streamingContext.awaitTermination();
    }
}