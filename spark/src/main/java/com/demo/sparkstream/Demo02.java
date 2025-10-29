package com.demo.sparkstream;

import org.apache.spark.SparkConf;
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
 * 演示 DStream数据集转换成RDD的操作，并演示作用域在Drvier端的变量
 *
 * @Author: 君墨笑
 * @Date: 2025/9/18 11:16
 */
public class Demo02 {

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
            //这里的rdd.collect();是一个Action算子，会触发底层RDD真正生成一个Job，并开始走DAG分析划分成多个Stage，每个Stage拆分成多个Task，并提交给Executor执行。
            //Executor计算完毕后，会将结果返回给Driver端
            List<Tuple2<String, Integer>> list = rdd.collect();
            //Driver端执行遍历打印，list.forEach这一段方法是在Driver端运行的
            list.forEach(tuple -> System.out.println(tuple._1 + ": " + tuple._2));
            //每执行一次微批都会计数一次
            int driverCount = count.incrementAndGet();
            System.out.println("======打印Driver端计数：" + driverCount + " =========");
        });

        //上述每获取到一个微批数据集之后，都会打包成一个RDD，然后将RDD封装成DStream对象，并交给Driver端进行处理。
        //所有对DStream对象定义计算逻辑操作都是在Driver端运行的，只有真正触发action算子，Drvier端才会开始DAG分析、拆分Stage、Task,
        // 然后将关于DStream对象操作的闭包函数发给Executor端，在Executor端进行计算。


        //启动程序开始微批流处理
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}