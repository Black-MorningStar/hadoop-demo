package com.demo.topn;

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
 * 计算出每个月温度最高的两天数据
 * 2019-6-1  39
 * 2019-5-21  33
 * 2019-6-1  38
 * 2019-6-2  31
 * 2018-3-11  18
 * 2018-4-23  22
 * 1970-8-23  23
 * 1970-8-8  32
 *
 * =====正确输出结果=======
 * 1970-8-23	23
 * 1970-8-8	3
 * 2018-3-11	18
 * 2018-4-23	22
 * 2019-5-21	33
 * 2019-6-1	39
 * 2019-6-2	31
 */
public class TopNMain {

    public static void main(String[] a) throws IOException, InterruptedException, ClassNotFoundException {
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
        //设置MapperTask的key排序比较器
        job.setSortComparatorClass(TopNComparator.class);
        //设置Mapper的key-value输出类型
        job.setMapOutputKeyClass(TopNKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Reduce类
        job.setReducerClass(TopNReducer.class);
        //设置reduce的分组比较器
        job.setGroupingComparatorClass(TopNGroupingComparator.class);
        //设置reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置计算文件的HDFS输入目录,默认会把目录下所有文件进行计算
        Path inPath = new Path("/topN/data");
        TextInputFormat.addInputPath(job,inPath);
        //设置计算结果HDFS的输出目录,必须是不存在的目录
        Path outPath = new Path("/topN/result");
        TextOutputFormat.setOutputPath(job,outPath);
        //设置计算程序Jar包所在位置,客户端会自动上传JAR包到HDFS上
        job.setJar("/Users/pengshaoxiang/IdeaProject/hadoop-demo/mapreduce/target/mapreduce-1.0-SNAPSHOT.jar");
        //提交任务
        job.waitForCompletion(true);
    }
}
