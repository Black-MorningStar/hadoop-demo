package com.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.io.IOException;

/**
 * 计算出每个月温度最高的两天数据
 * 2019-6-1  39
 * 2019-5-21  33
 * 2019-6-1  38
 * 2019-6-2  31
 * 2018-3-11  18
 * 2018-4-23  22
 * 1970-8-23  23
 * 1970-8-8  32
 */
public class TopNMain {

    public static void main(String[] a) throws IOException {
        //读取本地配置
        Configuration configuration = new Configuration(true);
        //设置ReduceTask的数量 (并行度)
        configuration.set(MRJobConfig.NUM_REDUCES,"1");
        //根据配置实例化一个Job对象
        Job job = Job.getInstance(configuration,"TopNMain");
        //设置Job的执行主类
        job.setJarByClass(TopNMain.class);
        //设置Mapper类
        job.setMapperClass(TopNMapper.class);
        //设置MapperTask的分区器
        job.setPartitionerClass(TopNPartitioner.class);
        //设置MapperTask的比较器

        //设置Mapper的key-value输出类型
        job.setMapOutputKeyClass(TopNKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Reduce类
        //job.setReducerClass(WordCountReduce.class);


    }
}
