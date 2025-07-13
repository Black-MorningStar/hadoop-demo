package com.demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class WordCountMain {

    public static void main(String[] args) throws Exception {
        //读取本地的yarn、mapred配置文件
        Configuration configuration = new Configuration(true);
        //设置hbase连接
        configuration.set("hbase.zookeeper.quorum", "localhost");
        //设置输出到Hbase的表
        /*configuration.set("mapreduce.outputformat.class", TableOutputFormat.class.getName());
        configuration.set(TableOutputFormat.OUTPUT_TABLE, "wordcount");*/
        //根据配置实例化一个Job对象
        Job job = Job.getInstance(configuration,"myWordCountJob");
        //设置Job的执行主类
        job.setJarByClass(WordCountMain.class);
        //设置Mapper类
        job.setMapperClass(WordCountMapper.class);
        //设置Mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Mapper 计算文件的HDFS输入路径,默认会把目录下所有文件进行计算
        Path inPath = new Path("/wc/data/");
        TextInputFormat.addInputPath(job,inPath);

        //设置reducer类
        TableMapReduceUtil.initTableReducerJob("wordcount",WordCountReduce.class,job,null);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        //设置计算程序Jar包所在位置,客户端会自动上传JAR包到HDFS上
        job.setJar("/Users/pengshaoxiang/IdeaProject/hadoop-demo/hbase/target/hbase-1.0-SNAPSHOT.jar");
        job. waitForCompletion(true);
    }
}
