package com.demo.sparksql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * SparkSQL 输出数据集
 *
 * @Author: 君墨笑
 * @Date: 2025/10/11 16:17
 */
public class SparkSqlWrite {

    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder().appName("SparkSqlRead").master("local[*]").getOrCreate();
        //读取CSV文件
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType);
        Dataset<Row> csv = sparkSession.read()
                .option("header", "true") //读取时候，第一行是标题行
                .schema(schema) //定义约束
                .csv("hdfs://localhost:9000/sparksql/file/testCSV.csv");
        csv.createTempView("person");
        Dataset<Row> sql = sparkSession.sql("select *,concat(name,'-',age) as hobby  from person");
        sql.printSchema();
        sql.write()
                .mode("overwrite")
                .option("header", "true")
                .csv("hdfs://localhost:9000/sparksql/write");
    }
}