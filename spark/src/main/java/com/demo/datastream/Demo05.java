package com.demo.datastream;

import jdk.xml.internal.XMLSecurityManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 演示有状态的计算
 * 微批计算是只对于一个批次的数据计算出结果
 * 窗口计算是可以将多个批次合并成一个窗口，可以对一个窗口的数据计算出结果
 * 如果我们想要得到无界数据流整体的计算结果，那么就需要有状态的计算。
 * 有状态的计算本质：是将历史计算的结果保存下来，新批次的数据可以读取到历史计算结构，然后结合历史计算结果做合并计算

 *
 * @Author: 君墨笑
 * @Date: 2025/9/19 15:04
 */
public class Demo05 {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("dataStream").setMaster("local[*]");
        //每2秒是一个批次，进行微批计算
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        streamingContext.sparkContext().setLogLevel("ERROR");

        //有状态的计算必须要设置数据保存目录,历史计算的结果会保存在这里
        //底层其实就是RDD的持久化方法
        streamingContext.checkpoint("hdfs://localhost:9000/spark/checkpoint");

        JavaReceiverInputDStream<String> dataStream = streamingContext.socketTextStream("localhost", 9666);
        JavaPairDStream<String, Integer> mapDs = dataStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToPair(word -> new Tuple2<>(word, 1));
        //类似于reduceByKey，按照Key做聚合计算，只不过是有状态的计算，会合并历史计算结果做合并计算
        //List<V> values 当前该批次的key的数据值集合
        //state 历史该key的计算结果值
        JavaPairDStream<String, Integer> wordCount = mapDs.updateStateByKey((values, state) -> {
            //历史计算结果
            int oldCount = state.orElse(0);
            //按照key对当前批次的数据做计数
            int sum = values.stream().mapToInt(it -> it.intValue()).sum();
            //将历史计算结果与当前批次计算结果做合并计算
            return Optional.of(sum + oldCount);
        });
        wordCount.print();

        //对于上述updateStateByKey函数，会先根据Key分组在调用updateStateByKey，入参里会把该批次中某个key所有的数据值作为一个集合传入，如果该批次中这个key的数据量很大，可能会造成内存溢出的问题
        //因此updateStateByKey函数已经不推荐使用了
        //mapWithState函数则是类似于流式处理的方式，该批次中的每条数据都会调用一次mapWithState函数进行处理，在函数mapWithState内部对于每一条数据进行合并计算处理，这样大大减少内存消耗。
        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                (key, value, state) -> {
                    // 当前key对应的value
                    Integer valueInt = value.orElse(0);
                    // 获取历史状态计算结果
                    int oldValue = state.exists() ? state.get() : 0;
                    // 更新状态
                    int newState = valueInt + oldValue;
                    state.update(newState);
                    // 返回结果
                    return new Tuple2<>(key, newState);
                };
        // 构建 StateSpec
        StateSpec<String, Integer, Integer, Tuple2<String, Integer>> stateSpec = StateSpec.function(mappingFunc);
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> mapState = mapDs.mapWithState(stateSpec);
        mapState.print();

        //启动程序开始微批流处理
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}