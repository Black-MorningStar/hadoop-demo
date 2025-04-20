package com.demo.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义key的分区器
 */
public class TopNPartitioner extends Partitioner<TopNKey, IntWritable> {

    @Override
    public int getPartition(TopNKey topNKey, IntWritable intWritable, int numPartitions) {
        //相同年月分配成一组,需要注意数据倾斜的问题，防止大量的数据分配在同一个分区
        String key = topNKey.getYear() + "-" + topNKey.getMonth();
        return key.hashCode() % numPartitions;
    }
}
