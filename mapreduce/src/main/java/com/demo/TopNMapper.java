package com.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Map处理程序
 */
public class TopNMapper extends Mapper<Object, Text, TopNKey, IntWritable> {

    //定义成为成员变量，不放在方法内定义，是防止每行数据调用map方法的时候都会new一个新对象造成频繁GC
    //而数据输出的时候会把key序列化输出出去
    private TopNKey topNKey = new TopNKey();
    private IntWritable one = new IntWritable(1);

    /**
     * 每一行数据调用一次map方法计算处理
     */
    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        //将一行行数据处理成对应输出的key进行输出
        String[] split = value.toString().split(" ");
        String date = split[0];
        String temperature = split[1];
        String[] dataSplit = date.split("-");
        topNKey.setYear(Integer.valueOf(dataSplit[0]));
        topNKey.setMonth(Integer.valueOf(dataSplit[1]));
        topNKey.setDay(Integer.valueOf(dataSplit[2]));
        topNKey.setTemperature(Integer.valueOf(temperature));
        context.write(topNKey,one);
    }
}
