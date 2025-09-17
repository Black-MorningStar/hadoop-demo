package com.demo.dataset;


import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.catalyst.plans.JoinType;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: 君墨笑
 * @Date: 2025/9/10 14:35
 */
public class DataSetMain {

    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder().appName("DataSetMain").master("local[*]").getOrCreate();
        List<Person> peopleList = Arrays.asList(
                new Person(1, "Alice", 23),
                new Person(2, "Bob", 31),
                new Person(3, "Charlie", 17),
                new Person(4, "lisi", 17),
                new Person(5, "wangwu", 22)
        );
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        //创建一个DataSet
        Dataset<Person> personDS = sparkSession.createDataset(peopleList, personEncoder);

        //过滤操作
        //Dataset<Person> filter = personDS.filter("age > 18").show();

        //映射操作
        //Dataset<String> map = personDS.filter("age > 18").map((MapFunction<Person, String>) Person::getName, Encoders.STRING()).show();

        //制定列查询 DataFrame操作
        //Dataset<Row> select = personDS.select("name", "age").show();

        //给数据集增加一列
        //Dataset<Row> isAult = personDS.withColumn("isAult", personDS.col("age").gt(18)).show();

        //分组统计操作
        //isAult.groupBy("isAult").count().show();

        //排序操作
        //personDS.orderBy(personDS.col("age").desc()).show();

        //去重操作
        //personDS.select("age").distinct().show();

        //关联操作
        /*List<Hobby> hobbyList = Arrays.asList(
                new Hobby(1, "Alice", "吃饭"),
                new Hobby(2, "Bob", "喝酒"),
                new Hobby(3, "Charlie", "唱歌"),
                new Hobby(4, "lisi", "打牌")
        );*/
        Encoder<Hobby> hobbyEncoder = Encoders.bean(Hobby.class);
        //创建一个DataSet
        /*Dataset<Hobby> hobbyDS = sparkSession.createDataset(hobbyList, hobbyEncoder);
        Dataset<Row> joinDS = personDS.join(hobbyDS, "name");
        joinDS.select("name","age","description").show();*/

        //SQL操作，必须先要注册临时视图
        personDS.createTempView("person");
        String db = sparkSession.catalog().currentDatabase();
        System.out.println("======" + db + "======");
        Dataset<Table> tableDataset = sparkSession.catalog().listTables();
        tableDataset.show();

        Dataset<Row> sql = sparkSession.sql("select * from person order by id desc");
        sql.show();
    }
}