package com.demo.sparksql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * SparkSql 读取数据源
 *
 * @Author: 君墨笑
 * @Date: 2025/10/9 15:50
 */
public class SparkSqlRead {

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
        /*System.out.println("===== Schema =====");
        csv.printSchema();
        System.out.println("===== Data =====");
        csv.show(false);
        System.out.println("CSV Count: " + csv.count());*/
        //csv.filter(csv.col("age").gt(18)).select("id", "name","age").show();
        //csv.select("id", "name","age").where(csv.col("age").gt(18)).show();
        csv.createTempView("person");
        Dataset<Row> sql = sparkSession.sql("select id,name,age,concat(name,'-',id) as hobby from person where age > 18");
        sql.printSchema();
    }
}