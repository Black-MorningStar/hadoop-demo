package com.demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Author: 君墨笑
 * @Date: 2025/4/7 20:13
 */
public class WordCountMain {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //读取本地的yarn、mapred配置文件
        Configuration configuration = new Configuration(true);
        //设置ReduceTask的数量 (并行度)
        configuration.set(MRJobConfig.NUM_REDUCES,"1");
        //根据配置实例化一个Job对象
        Job job = Job.getInstance(configuration,"myWordCountJob");
        //设置Job的执行主类
        job.setJarByClass(WordCountMain.class);
        //设置Mapper类
        job.setMapperClass(WordCountMapper.class);
        //设置Reduce类
        job.setReducerClass(WordCountReduce.class);
        //设置Mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置计算文件的HDFS输入目录,默认会把目录下所有文件进行计算
        Path inPath = new Path("/wc/data/");
        TextInputFormat.addInputPath(job,inPath);
        //设置计算结果HDFS的输出目录,必须是不存在的目录
        Path outPath = new Path("/wc/result/");
        TextOutputFormat.setOutputPath(job,outPath);
        //设置计算程序Jar包所在位置,客户端会自动上传JAR包到HDFS上
        job.setJar("/Users/pengshaoxiang/IdeaProject/hadoop-demo/mapreduce/target/mapreduce-1.0-SNAPSHOT.jar");
        //提交任务
        job. waitForCompletion(true);
    }
}