package com.demo.analog;

import com.demo.etllog.EtlLogMain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @Author: 君墨笑
 * @Date: 2025/7/15 14:27
 */
public class AnalyzeMain {

    public static void main(String[] a) throws IOException, InterruptedException, ClassNotFoundException {
        //读取本地的yarn、mappred配置文件
        Configuration configuration = new Configuration(true);
        //设置hbase连接
        configuration.set("hbase.zookeeper.quorum", "localhost");
        //根据配置实例化一个Job对象
        Job job = Job.getInstance(configuration,"analyzeLogJob");
        //设置Job的执行主类
        job.setJarByClass(AnalyzeMain.class);
        //设置Hbase的读取Mapper
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("param"), Bytes.toBytes("plat"));
        scan.addColumn(Bytes.toBytes("param"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("param"), Bytes.toBytes("time"));
        TableMapReduceUtil.initTableMapperJob("requestlog", scan, AnalyzeMapper.class, AnalyzeKey.class, IntWritable.class, job);
        //设置mysql的输出
        job.setReducerClass(AnalyzeReducer.class);
        job.setOutputFormatClass(MySQLOutputFormat.class);
        job.setOutputKeyClass(AnalyzeKey.class);
        job.setOutputValueClass(IntWritable.class);
        //设置计算程序Jar包所在位置,客户端会自动上传JAR包到HDFS上
        job.setJar("/Users/pengshaoxiang/IdeaProjects/hadoop-demo/hbase/target/hbase-1.0-SNAPSHOT.jar");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}