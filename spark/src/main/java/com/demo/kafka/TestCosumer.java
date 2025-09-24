package com.demo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: 君墨笑
 * @Date: 2025/9/23 14:49
 */
public class TestCosumer {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "testTopic3";
    public static final String groupId = "testCosumerGroup5";
    public static final String clientId = "testCosumer02";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig(){
        Properties props = new Properties();
        //配置反序列化key-value
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        //配置kafka服务器地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        //配置消费组ID和消费者ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        //配置自动提交消费偏移量
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        //配置每次poll拉取的最大消息数量
        //props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");
        return props;
    }

    public static void main(String[] args) {
        Properties props = initConfig();
        //创建消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅主题
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
           //在绑定的分区被回收前调用,可以用来持久化消费过的offset
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("触发了onPartitionsRevoked====");
            }

            //新分区分配给当前消费者后调用,可以用来重置offset
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("触发了onPartitionsAssigned====");
                collection.forEach(it -> {
                    System.out.println("分配了分区: " + it.partition());
                    //consumer.seek(it,10);
                });
            }
        });
        try {
            while (isRunning.get()) {
                //拉取消息开始消费
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(2000));
                if (records.isEmpty()) {
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic = " + record.topic()
                            + ", partition = "+ record.partition()
                            + ", offset = " + record.offset());
                    System.out.println("key = " + record.key()
                            + ", value = " + record.value());
                }
                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}