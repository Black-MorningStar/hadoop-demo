package com.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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
        //设置文件的输入
        Path inPath = new Path(args[0]);
        TextInputFormat.addInputPath(job,inPath);
        //设置文件的输出
        Path outPath = new Path(args[1]);
        TextOutputFormat.setOutputPath(job,outPath);
        //设置计算程序Jar包所在位置,客户端会自动上传JAR包到HDFS上
        job.setJar("XXXX/XXXX/xxx.jar");
        //提交任务
        job. waitForCompletion(true);
    }
}