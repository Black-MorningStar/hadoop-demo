package com.demo.sparkstream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 演示窗口函数操作
 *
 * @Author: 君墨笑
 * @Date: 2025/9/19 14:29
 */
public class Demo04 {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("dataStream").setMaster("local[*]");
        //每2秒是一个批次，进行微批计算
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        streamingContext.sparkContext().setLogLevel("ERROR");

        JavaReceiverInputDStream<String> dataStream = streamingContext.socketTextStream("localhost", 9666);

        //窗口操作
        //Durations.seconds(6) 定义窗口长度,窗口长度一定是批次的整数倍,比如批次是2秒的数据算一个批次,这里定义的6,代表窗口的长度是6/2=3个批次
        //代表每次窗口都处理3个批次
        //Durations.seconds(2) 定义窗口滑动步长,也可以理解是触发频率,这里的步长一定是批次的整数倍，
        // 这里是每2秒触发一次窗口计算,每次计算的窗口包含3个批次的数据
        /*JavaDStream<String> window = dataStream.window(Durations.seconds(6), Durations.seconds(2));
        JavaPairDStream<String, Integer> wordCount = window.mapToPair(value -> new Tuple2<>(value, 1)).reduceByKey((v1, v2) -> v1 + v2);
        wordCount.print();*/

        JavaPairDStream<String, Integer> map = dataStream.flatMap(value -> Arrays.stream(value.split(" ")).iterator())
                .mapToPair(value -> new Tuple2<>(value, 1));
        //这个reduceByKeyAndWindow函数，每次都会计算一次窗口内的全量的批次数据做reduce计算。比如窗口定义的6/2=3个批次
        //那么该函数则会每2秒将3个批次的全量数据做一次计算
        /*JavaPairDStream<String, Integer> wordCount = map.reduceByKeyAndWindow((v1, v2) -> v1 + v2, Durations.seconds(6), Durations.seconds(2));
        wordCount.print();*/


        //这个带反向函数的reduceByKeyAndWindow函数，比上面那个效率要高，因为它不会每次都计算窗口内全量批次的数据，
        //而是每次窗口滑动之后，都会依靠上个窗口的结果数据，然后只合并计算新进入窗口的批次数据，以及去除掉滑出窗口的批次数据。
        //第一个函数是合并函数，第二个函数是去除函数
        //很明显看出，这是一个有状态的计算，因为每次都要存储上个窗口计算的结果数据，方便下一次窗口滑动使用
        //有状态的计算，一定会将历史计算的结果数据存在某一个地方 (内存、硬盘、分布式文件系统等)

        //设置外部保存点,有状态计算会将历史计算结果数据保存到这里。底层其实就是RDD的持久化方法
        streamingContext.checkpoint("hdfs://localhost:9000/spark/checkpoint");
        JavaPairDStream<String, Integer> wordCount = map.reduceByKeyAndWindow((v1, v2) -> v1 + v2, (v1, v2) -> v1 - v2,
                Durations.seconds(6), Durations.seconds(2));
        wordCount.print();

        //启动程序开始微批流处理
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}