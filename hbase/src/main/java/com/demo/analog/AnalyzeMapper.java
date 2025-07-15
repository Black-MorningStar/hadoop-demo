package com.demo.analog;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

/**
 * @Author: 君墨笑
 * @Date: 2025/7/15 12:05
 */
public class AnalyzeMapper extends TableMapper<AnalyzeKey, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final static AnalyzeKey analyzeKey = new AnalyzeKey();

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String name = Bytes.toString(value.getValue(Bytes.toBytes("param"), Bytes.toBytes("name")));
        String plat = Bytes.toString(value.getValue(Bytes.toBytes("param"), Bytes.toBytes("plat")));
        String time = Bytes.toString(value.getValue(Bytes.toBytes("param"), Bytes.toBytes("time")));
        analyzeKey.setName(name);
        analyzeKey.setType(AnalyzeKey.USER);
        analyzeKey.setTime(time);
        context.write(analyzeKey, one);
        analyzeKey.setName(plat);
        analyzeKey.setType(AnalyzeKey.PLAT);
        analyzeKey.setTime(time);
        context.write(analyzeKey, one);
    }
}