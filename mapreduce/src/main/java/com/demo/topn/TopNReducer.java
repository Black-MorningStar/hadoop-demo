package com.demo.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reduce处理程序
 */
public class TopNReducer extends Reducer<TopNKey, IntWritable, Text,IntWritable> {

    private IntWritable valueResult = new IntWritable();
    private Text keyResult = new Text();

    /**
     * @param key map处理之后输出的key
     * @param values 由于是一组数据调用一次reduce方法，这里就是按照key分组的values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(TopNKey key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        /**
         * 因为按照年月进行分组，相同年月的key会分在一组，并且之前已经将同一分组内的数据按照温度进行倒序排序
         * 所以每个分组内的第一条数据就是温度最高的数据，直接第一条数据输出即可
         * 对于第二条数据，因为我们计算的是一个月内温度最高的两天，所以跟第一条数据在同一天的数据不算在内，因此我们需要找出接下来的数据不等于
         * 第一条的日期的数据
         */
        //第一条数据的日期
        int count = 0;
        while (iterator.hasNext()) {
            IntWritable next = iterator.next();
            //代表第一条数据
            if (count == 0) {
                keyResult.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                valueResult.set(key.getTemperature());
                context.write(keyResult, valueResult);
                count = key.getDay();
            } else {
                //判断是否跟第一条数据的日期相同，相同则跳过，不相同则输出结果
                if (count != key.getDay()) {
                    keyResult.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                    valueResult.set(key.getTemperature());
                    context.write(keyResult, valueResult);
                    //跳出第一组数据的迭代
                    break;
                }
            }
        }
    }
}
