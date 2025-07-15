package com.demo.analog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: 君墨笑
 * @Date: 2025/7/15 15:12
 */
public class MySQLRecordWriter extends RecordWriter<AnalyzeKey, IntWritable> {

    private Connection connection;
    private PreparedStatement statement;
    private static final int BATCH_SIZE = 1000; // 可根据实际情况调整
    private int batchCount = 0;

    public MySQLRecordWriter(TaskAttemptContext context) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/demo?useSSL=false&serverTimezone=UTC",
                    "root", "psx7552933"
            );
            connection.setAutoCommit(false);
            statement = connection.prepareStatement("INSERT INTO log_event (log_type, log_name, log_time,log_count) VALUES (?, ?, ?,?)");
        } catch (Exception e) {
            throw new RuntimeException("连接Mysql失败", e);
        }
    }
    @Override
    public void write(AnalyzeKey key, IntWritable value) throws IOException, InterruptedException {
        try {
            String type = key.getType();
            statement.setString(1, type);
            statement.setString(2, key.getName());
            statement.setString(3, key.getTime());
            statement.setInt(4, value.get());
            statement.addBatch();
            batchCount++;
            if (batchCount >= BATCH_SIZE) {
                statement.executeBatch();
                batchCount = 0;
            }
        } catch (SQLException e) {
            throw new IOException("Failed to write to MySQL", e);
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            if (batchCount > 0) {
                statement.executeBatch();
            }
            connection.commit();
            statement.close();
            connection.close();
        } catch (SQLException e) {
            throw new IOException("Failed to close MySQL connection", e);
        }
    }
}