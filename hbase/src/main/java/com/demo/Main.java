package com.demo;

import com.demo.base.HbaseClient;
import com.demo.base.HbaseData;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        //创建一张表
         //HbaseClient.createTable("requestlog", Lists.newArrayList("request","param"));
        List<HbaseData> requestlog = HbaseClient.getAllData("requestlog");
        for (HbaseData result : requestlog) {
            String rowKey = Bytes.toString(result.getRowKey());
            List<HbaseData.HbaseDataDetail> data = result.getData();
            System.out.println("RowKey：" + rowKey);
            for (HbaseData.HbaseDataDetail cell : data) {
                String family = Bytes.toString(cell.getColumnFamily());
                String qualifier = Bytes.toString(cell.getColumnName());
                String value = Bytes.toString(cell.getColumnValue());
                System.out.println("family：" + family + " qualifier: " + qualifier + " value: " + value);
            }
            System.out.println("============");
        }
        /*ArrayList<HbaseData> list = new ArrayList<>();
        HbaseData data1 = new HbaseData();
        data1.setRowKey(Bytes.toBytes("1000000001"));
        ArrayList<HbaseData.HbaseDataDetail> detailList1 = new ArrayList<>();
        HbaseData.HbaseDataDetail hbaseDataDetail1 = new HbaseData.HbaseDataDetail();
        hbaseDataDetail1.setColumnFamily(Bytes.toBytes("info"));
        hbaseDataDetail1.setColumnName(Bytes.toBytes("name"));
        hbaseDataDetail1.setColumnValue(Bytes.toBytes("张三"));
        HbaseData.HbaseDataDetail hbaseDataDetail2 = new HbaseData.HbaseDataDetail();
        hbaseDataDetail2.setColumnFamily(Bytes.toBytes("hobby"));
        hbaseDataDetail2.setColumnName(Bytes.toBytes("age"));
        hbaseDataDetail2.setColumnValue(Bytes.toBytes(10));
        detailList1.add(hbaseDataDetail1);
        detailList1.add(hbaseDataDetail2);
        data1.setData(detailList1);

        HbaseData data2 = new HbaseData();
        data2.setRowKey(Bytes.toBytes("1000000002"));
        ArrayList<HbaseData.HbaseDataDetail> detailList2 = new ArrayList<>();
        HbaseData.HbaseDataDetail hbaseDataDetail3 = new HbaseData.HbaseDataDetail();
        hbaseDataDetail3.setColumnFamily(Bytes.toBytes("info"));
        hbaseDataDetail3.setColumnName(Bytes.toBytes("name"));
        hbaseDataDetail3.setColumnValue(Bytes.toBytes("李四"));
        HbaseData.HbaseDataDetail hbaseDataDetail4 = new HbaseData.HbaseDataDetail();
        hbaseDataDetail4.setColumnFamily(Bytes.toBytes("hobby"));
        hbaseDataDetail4.setColumnName(Bytes.toBytes("age"));
        hbaseDataDetail4.setColumnValue(Bytes.toBytes(20));
        detailList2.add(hbaseDataDetail3);
        detailList2.add(hbaseDataDetail4);
        data2.setData(detailList2);
        HbaseClient.putData("person",Lists.newArrayList(data1,data2));*/

        /*HbaseQuery hbaseQuery = new HbaseQuery();
        hbaseQuery.setRowKey(Bytes.toBytes("1000000001"));
        HbaseQuery.QueryDetail queryDetail1 = new HbaseQuery.QueryDetail();
        queryDetail1.setColumnFamily(Bytes.toBytes("info"));
        queryDetail1.setColumnName(Bytes.toBytes("name"));
        HbaseQuery.QueryDetail queryDetail2 = new HbaseQuery.QueryDetail();
        queryDetail2.setColumnFamily(Bytes.toBytes("hobby"));
        queryDetail2.setColumnName(Bytes.toBytes("age"));
        hbaseQuery.setDetails(Lists.newArrayList(queryDetail1,queryDetail2));


        HbaseQuery hbaseQuery2 = new HbaseQuery();
        hbaseQuery2.setRowKey(Bytes.toBytes("1000000002"));
        HbaseQuery.QueryDetail queryDetail3 = new HbaseQuery.QueryDetail();
        queryDetail3.setColumnFamily(Bytes.toBytes("info"));
        queryDetail3.setColumnName(Bytes.toBytes("name"));
        hbaseQuery2.setDetails(Lists.newArrayList(queryDetail3));

        List<HbaseData> person = HbaseClient.getByRowKey("person", Lists.newArrayList(hbaseQuery, hbaseQuery2));
        person.forEach(it -> {
            System.out.println("RowKey: " + Bytes.toString(it.getRowKey()));
            it.getData().forEach(ot -> {
                String columnName = Bytes.toString(ot.getColumnName());
                if (columnName.contentEquals("name")) {
                    System.out.println(Bytes.toString(ot.getColumnFamily()) + "-" + columnName + ": " + Bytes.toString(ot.getColumnValue()));
                }
                if (columnName.contentEquals("age")) {
                    System.out.println(Bytes.toString(ot.getColumnFamily()) + "-" + columnName + ": " + Bytes.toInt(ot.getColumnValue()));
                }
            });
        });*/
    }
}