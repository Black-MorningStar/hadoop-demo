package com.demo;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WordCountReduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    private static final byte[] CF = Bytes.toBytes("cf");
    private static final byte[] QUALIFIER = Bytes.toBytes("count");

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        Put put = new Put(Bytes.toBytes(key.toString()));
        put.addColumn(CF, QUALIFIER, Bytes.toBytes(String.valueOf(sum)));
        context.write(null, put); // key=null 表示我们不需要 TableOutputFormat 处理主键
    }
}
