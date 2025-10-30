package com.demo.structstreaming;

import com.demo.sparksql.Person;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

/**
 * @Author: 君墨笑
 * @Date: 2025/10/29 16:56
 */
public class FileSink {

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

        map.writeStream()
                .format("csv")
                .outputMode("append")
                .option("checkpointLocation","file:///Users/pengshaoxiang/Documents/checkpoint")
                .option("path","file:///Users/pengshaoxiang/Documents/sparksink")
                .option("header","true")
                .start()
                .awaitTermination();
    }
}