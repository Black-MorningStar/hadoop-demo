package com.demo.sparksql;

import org.apache.spark.sql.SparkSession;

/**
 * SparkSQL和Hive元数据服务的集成
 *
 * @Author: 君墨笑
 * @Date: 2025/9/15 15:49
 */
public class SparkSqlHive {

    public static void main(String[] a) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkSqlHiveMain")
                .master("local[*]")
                .config("hive.metastore.uris","thrift://localhost:9083") //配置Hive的元数据服务地址
                .enableHiveSupport() //开启和Hive的集成
                .getOrCreate();

        sparkSession.catalog().listDatabases().show();
        sparkSession.catalog().listTables().show();
        //sparkSession.sql("select * from log_info limit 10").show();
    }
}