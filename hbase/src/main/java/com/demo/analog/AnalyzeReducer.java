package com.demo.analog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * @Author: 君墨笑
 * @Date: 2025/7/15 15:31
 */
public class AnalyzeReducer extends Reducer<AnalyzeKey, IntWritable, AnalyzeKey, IntWritable> {

    IntWritable count = new IntWritable();
    @Override
    protected void reduce(AnalyzeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            sum = sum + iterator.next().get();
        }
        count.set(sum);
        context.write(key, count);
    }
}