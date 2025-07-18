package com.demo.etllog;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.UUID;

public class AnaLogMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {


    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        //解析每一行日志
        String[] split = value.toString().split("\\^A");
        String remoteIp = split[0];
        Double time = Double.valueOf(split[1]) * 1000;
        long longTime = time.longValue();
        String requestHost = split[2];
        String[] urlSplit = split[3].split("\\?");
        String url = urlSplit[0];
        String[] split2 = urlSplit[1].split("=");
        String param = split2[1];
        //写入hbase
        String rowKey = UUID.randomUUID().toString();
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("request"), Bytes.toBytes("remoteIp"), Bytes.toBytes(remoteIp));
        put.addColumn(Bytes.toBytes("request"), Bytes.toBytes("requestTime"), Bytes.toBytes(String.valueOf(longTime)));
        put.addColumn(Bytes.toBytes("request"), Bytes.toBytes("requestHost"), Bytes.toBytes(requestHost));
        put.addColumn(Bytes.toBytes("request"), Bytes.toBytes("requestUrl"), Bytes.toBytes(url));
        put.addColumn(Bytes.toBytes("param"), Bytes.toBytes("name"), Bytes.toBytes(param));
        context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
    }
}
