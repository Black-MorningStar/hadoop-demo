package com.demo.etllog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class EtlLogMain {


    public static void main(String[] a) throws Exception {
        //读取本地的yarn、mappred配置文件
        Configuration configuration = new Configuration(true);
        //设置hbase连接
        configuration.set("hbase.zookeeper.quorum", "localhost");
        //根据配置实例化一个Job对象
        Job job = Job.getInstance(configuration,"etlLogJob");
        //设置Job的执行主类
        job.setJarByClass(EtlLogMain.class);
        //设置Mapper类
        job.setMapperClass(EtlLogMapper.class);
        //设置Mapper的输出类型
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //设置Mapper 计算文件的HDFS输入路径,默认会把目录下所有文件进行计算
        Path inPath = new Path("/flume/logs/2025-07-15/");
        TextInputFormat.addInputPath(job,inPath);
        //设置reducer类
        TableMapReduceUtil.initTableReducerJob("requestlog", null,job,null);
        //设置计算程序Jar包所在位置,客户端会自动上传JAR包到HDFS上
        job.setJar("/Users/pengshaoxiang/IdeaProjects/hadoop-demo/hbase/target/hbase-1.0-SNAPSHOT.jar");
        job. waitForCompletion(true);
    }
}
