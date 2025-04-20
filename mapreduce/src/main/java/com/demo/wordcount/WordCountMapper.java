package com.demo.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author: 君墨笑
 * @Date: 2025/4/7 20:25
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    /**
     * @param key 由于使用的TextInputFormat作为输入,该类会把文本文件按行切割成一条条记录调用Map方法。key是每行文本在文件中的起始数据偏移量位置。
     * @param value 被切割的一条条文本内容。
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}