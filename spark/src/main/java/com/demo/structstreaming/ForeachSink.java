package com.demo.structstreaming;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

/**
 * @Author: 君墨笑
 * @Date: 2025/10/30 16:37
 */
public class ForeachSink {

    public static void main(String[] args) throws AnalysisException, TimeoutException, StreamingQueryException {
        //StructStreaming依旧使用 sparkSession、DataSet、DataFrame编程模型
        SparkSession sparkSession = SparkSession.builder().appName("SparkStructStreaming").master("local[*]").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        StructType schema = new StructType()
                .add("word", DataTypes.StringType)
                .add("time",DataTypes.StringType);

        //从文件系统目录中读取数据,每次目录下新增一个文件会视为新批次读取,原有文件里的内容变更不会读取到
        //这里的粒度是单个文件为粒度
        Dataset<String> data = sparkSession.readStream()
                .text("file:///Users/pengshaoxiang/Documents/sparksource").as(Encoders.STRING());
        Dataset<Row> map = data.map((MapFunction<String, Row>) line -> {
                    String[] parts = line.split(",");
                    return RowFactory.create(parts[0], parts[1]);
                }, RowEncoder.apply(
                        schema))
                .withColumn("eventTime", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss"))
                .select("word", "eventTime");
        Dataset<Row> wordCount = map.groupBy("word").count();
        //因为这里对外输出的时候用的是SparkSQL模式的对外输出，每个分区都会写一个文件
        //因此合并分区为1，确保每个批次输出的文件只有1个
        wordCount.coalesce(1).writeStream()
                .outputMode("complete")
                .option("checkpointLocation","file:///Users/pengshaoxiang/Documents/checkpoint")
                        .foreachBatch((batchDF, batchId) -> {
                            //批次号一般用来做幂等，保证一个批次只对外输出一次。
                            System.out.println("输出批次号: " + batchId);
                            batchDF.write()
                                    .mode("overwrite")
                                    .option("header", "true")
                                    .csv("file:///Users/pengshaoxiang/Documents/sparksink");
                        }).start().awaitTermination();
    }
}