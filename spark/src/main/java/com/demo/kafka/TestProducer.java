package com.demo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * @Author: 君墨笑
 * @Date: 2025/9/23 14:43
 */
public class TestProducer {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "testTopic3";

    public static Properties initConfig(){
        Properties props = new Properties();
        //配置kafka通信地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        //设置发送消息的key-value序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        //配置生产组的ID
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "testProducer");
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        //创建Producer实例
        //Producer实例是线程安全的，多个线程可以共用一个实例，它只是发送消息的作用
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        System.out.println("开始发送消息...");

        int count = 0;
        while (count <= 300) {
            try {
                //构建待发送消息体
                int num = new Random().nextInt(100);
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, "key-"+num,"Hello, Kafka!");
                //发送消息
                producer.send(record);
                //Thread.sleep(200);
            } catch (Exception e) {
                e.printStackTrace();
            }
            count++;
        }
    }
}