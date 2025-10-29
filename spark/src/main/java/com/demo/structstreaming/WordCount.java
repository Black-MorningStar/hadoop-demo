package com.demo.structstreaming;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

/**
 * @Author: 君墨笑
 * @Date: 2025/10/15 16:59
 */
public class WordCount {


    public static void main(String[] args) throws AnalysisException, TimeoutException, StreamingQueryException {
        //StructStreaming依旧使用 sparkSession、DataSet、DataFrame编程模型
        SparkSession sparkSession = SparkSession.builder().appName("SparkStructStreaming").master("local[*]").getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        StructType schema = new StructType()
                .add("word", DataTypes.StringType);

        //从文件系统目录中读取数据,每次目录下新增一个文件会视为新批次读取,原有文件里的内容变更不会读取到
        //这里的粒度是单个文件为粒度
        Dataset<Row> json = sparkSession.readStream()
                .schema(schema)
                .text("file:///Users/pengshaoxiang/Documents/sparkdata");
        json.createTempView("data");
        Dataset<Row> sql = sparkSession.sql("select word, count(*) as amount from data group by word");
        sql.writeStream()
                .format("console")
                //.trigger(Trigger.ProcessingTime(10000))
                //.trigger(Trigger.Once())
                //.trigger(Trigger.Continuous(10))
                .outputMode("complete")
                .option("checkpointLocation","file:///Users/pengshaoxiang/Documents/checkpoint")
                .start()
                .awaitTermination();
    }
}