package com.demo.analog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.File;
import java.io.IOException;

/**
 * @Author: 君墨笑
 * @Date: 2025/7/15 15:22
 */
public class MySQLOutputFormat extends OutputFormat<AnalyzeKey, IntWritable> {

    public MySQLOutputFormat() {
    }

    @Override
    public RecordWriter<AnalyzeKey, IntWritable> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new MySQLRecordWriter(context);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {
        // 可选检查，例如参数校验
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        return new MySQLCommitter();
    }
}