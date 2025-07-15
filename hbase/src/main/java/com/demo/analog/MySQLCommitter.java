package com.demo.analog;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @Author: 君墨笑
 * @Date: 2025/7/15 16:25
 */
public class MySQLCommitter extends OutputCommitter {

    public MySQLCommitter() {}

    @Override public void setupJob(JobContext jobContext) {}
    @Override public void setupTask(TaskAttemptContext taskContext) {}
    @Override public boolean needsTaskCommit(TaskAttemptContext taskContext) { return false; }
    @Override public void commitTask(TaskAttemptContext taskContext) {}
    @Override public void abortTask(TaskAttemptContext taskContext) {}
}