package com.demo.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: 君墨笑
 * @Date: 2025/4/7 20:36
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    /**
     * @param key map处理之后输出的key
     * @param values 由于是一组数据调用一次reduce方法，这里就是按照key分组的values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        int sum = 0;
        Iterator<IntWritable> iterator = values.iterator();
        //iterator.hasNext()迭代器会根据分组器判断下一条数据的key和当前数据的key是不是同一组，是就返回true，否则返回false
        //由于自定义分组器可以实现不同的key分配在同一组内，因此调用iterator.hasNext()方法，会把当前数据的key赋值到入参的key上，
        //因为入参的key是引用类型，因此在迭代的时候，其实key是一条条数据的key，是会变化的
        while (iterator.hasNext()) {
            sum += iterator.next().get();
        }
        result.set(sum);
        context.write(key, result);
    }
}