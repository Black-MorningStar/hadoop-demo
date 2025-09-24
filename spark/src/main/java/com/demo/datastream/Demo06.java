package com.demo.datastream;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;

/**
 * 演示Kafka作为SparkStreaming的数据源
 *
 * @Author: 君墨笑
 * @Date: 2025/9/22 19:31
 */
public class Demo06 {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "testTopic3";
    public static final String groupId = "testCosumerGroup15";
    public static final String clientId = "testCosumer15";
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("dataStream").setMaster("local[*]");
        //开启背压机制，开启后，Spark 会根据系统处理速度 动态调整拉取速率，避免堆积，或者一次性拉取数量过大
        //一般建议和 maxRatePerPartition 搭配使用。
        sparkConf.set("spark.streaming.backpressure.enabled","true");
        //配置 每个分区 每秒最多拉取多少条数据。
        //假设topic有3个partion，每2秒一个批次，那么这里拉取的数量= 2*3*5 = 30条数据
        sparkConf.set("spark.streaming.kafka.maxRatePerPartition","2");

        //每2秒是一个批次，进行微批计算
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        streamingContext.sparkContext().setLogLevel("ERROR");
        //配置kafka参数
        Map kafkaConfig = initConfig();
        //创建kafka数据流
        //默认每2秒拉取批次数据的时候，都是全量拉未消费的数据，如果生产者速度过快、或者消费者速度过慢，那么就会导致一个批次拉取的未消费数据量过大，导致内存溢出问题
        //在SparkStreaming中,kafka的MAX_POLL_RECORDS_CONFIG配置是不生效的，需要设置SparkStreaming本身的背压设置
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Lists.newArrayList(topic), kafkaConfig));
        JavaPairDStream<Integer, String> mapStream = directStream.mapToPair(record -> new Tuple2<>(record.partition(), record.topic() + "-" + record.partition() + "-" + record.offset() + ":" + record.value()));
        //mapStream.print();

        //由于自动提交偏移量关闭，因此我们需要手动提交维护偏移量。必须保证数据只计算一次(不可以少计算、重复计算)
        //一般的方法是在Driver端等批处理JOB执行完毕后，将计算结果拉回到Drvier端进行计算结果输出，然后手动提交偏移量
        //必须要保证计算结果输出操作和提交偏移量时在同一个事务内，比如计算结果输出到Mysql。必须要保证计算结果持久化和偏移量提交是在同一个事务内。
        mapStream.foreachRDD(rdd -> {
            List<Tuple2<Integer, String>> collect = rdd.collect();
            collect.forEach(it -> System.out.println(it._1 + ": " + it._2));
            System.out.println("当前批次处理完毕");
        });
        //通过最开始获得的KafkaStream可以得到该批次消息的偏移量以及进行方法提交
        directStream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            //必须在foreachRDD内部调用提交方法，因为这是在Driver端的批处理Job线程里执行的。
            ((CanCommitOffsets) directStream.inputDStream()).commitAsync(offsetRanges);
            System.out.println("当前批次偏移量提交完毕");
        });
        //这两个mapStream.foreachRDD、directStream.foreachRDD的方法都是在Drvier端的批处理线程里串行执行的

        //启动程序开始微批流处理
        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public static Map<String, Object> initConfig(){
        HashMap<String,Object> props = new HashMap<>();
        //配置反序列化key-value
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        //配置kafka服务器地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        //配置消费组ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //配置消费者ID
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        //配置自动提交消费偏移量,如果配置了false则需要在自己维护提交偏移量
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //配置消费偏移量设置earliest
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //配置每次poll最大拉取的消息数量,但是在SparkStream中，该配置不起作用
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2");
        return props;
    }
}