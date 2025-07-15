package com.demo.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class HbaseClient {


    //hbase的连接，在程序启动时创建、程序关闭时销毁，整个程序生命周期中共用一个
    private static Connection connection = null;

    static {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        try {
            connection = ConnectionFactory.createConnection(config);
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    /**
     * 创建一个表
     *
     * @param name 表名
     * @param columnFamily 列族名
     */
    public static void createTable(String name, List<String> columnFamily) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(name);

            //表存在则先删除
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            // 创建列族描述器
            ArrayList<ColumnFamilyDescriptor> list = new ArrayList<>();
            for (String cf : columnFamily) {
                list.add(ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build());
            }

            // 创建表描述器
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamilies(list)
                    .build();
            admin.createTable(tableDescriptor);
            System.out.println("表创建成功！");
        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            //，每次使用完毕需要关闭连接
            if (admin != null) {
                try {
                    admin.close();
                } catch (Exception e) {
                    System.out.println("关闭admin连接失败" + e.toString());
                }
            }
        }
    }

    /**
     * 删除一张表
     *
     * @param name 表名
     */
    public static void deleteTable(String name) {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(name);

            //表存在则先删除
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("表删除成功！");
            } else {
                System.out.println("表删除失败！表不存在");
            }
        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            //每次使用完毕需要关闭连接
            if (admin != null) {
                try {
                    admin.close();
                } catch (Exception e) {
                    System.out.println("关闭admin连接失败" + e.toString());
                }
            }
        }
    }


    /**
     * 新增/更新数据
     *
     * @param name 表名
     * @param dataList 数据
     */
    public static void putData(String name, List<HbaseData> dataList) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(name));
            ArrayList<Put> list = new ArrayList<>();
            dataList.forEach(data -> {
                Put put = new Put(data.getRowKey());
                List<HbaseData.HbaseDataDetail> dataDetail = data.getData();
                dataDetail.forEach(detail -> {
                    put.addColumn(detail.getColumnFamily(),detail.getColumnName(),detail.getColumnValue());
                });
                list.add(put);
            });
            table.put(list);
            System.out.println("插入数据完成！");
        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            //每次使用完毕需要关闭连接
            if (table != null) {
                try {
                    table.close();
                } catch (Exception e) {
                    System.out.println("关闭table连接失败" + e.toString());
                }
            }
        }
    }


    /**
     * 根据rowKey返回结果
     *
     * @param name 表名
     * @param query 查询条件
     * @return 结果
     */
    public static List<HbaseData> getByRowKey(String name, List<HbaseQuery> query) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(name));
            HashMap<String, List<HbaseQuery.QueryDetail>> queryMap = new HashMap<>();
            query.forEach(it -> {
                queryMap.put(Bytes.toString(it.getRowKey()),it.getDetails());
            });
            ArrayList<Get> list = new ArrayList<>();
            query.forEach(key -> {
                Get get = new Get(key.getRowKey());
                //精准设置查询的列族、列，减少数据量的查询和网络IO
                key.getDetails().forEach(detail -> {
                    get.addColumn(detail.getColumnFamily(),detail.getColumnName());
                });
                list.add(get);
            });
            Result[] results = table.get(list);
            ArrayList<HbaseData> result = new ArrayList<>();
            Arrays.stream(results).forEach(it -> {
                String rowKey = Bytes.toString(it.getRow());
                List<HbaseQuery.QueryDetail> queryDetails = queryMap.get(rowKey);
                HbaseData response = new HbaseData();
                ArrayList<HbaseData.HbaseDataDetail> detailList = new ArrayList<>();
                response.setData(detailList);
                response.setRowKey(it.getRow());
                queryDetails.forEach(detail -> {
                    byte[] value = it.getValue(detail.getColumnFamily(), detail.getColumnName());
                    HbaseData.HbaseDataDetail hbaseDataDetail = new HbaseData.HbaseDataDetail();
                    hbaseDataDetail.setColumnFamily(detail.getColumnFamily());
                    hbaseDataDetail.setColumnName(detail.getColumnName());
                    hbaseDataDetail.setColumnValue(value);
                    detailList.add(hbaseDataDetail);
                });
                result.add(response);
            });
            return result;
        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            //每次使用完毕需要关闭连接
            if (table != null) {
                try {
                    table.close();
                } catch (Exception e) {
                    System.out.println("关闭table连接失败" + e.toString());
                }
            }
        }
        return null;
    }

    public static List<HbaseData> getAllData(String tableName) {
        Table table = null;
        ArrayList<HbaseData> list = new ArrayList<>();
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                String rowKey = Bytes.toString(result.getRow());
                HbaseData hbaseData = new HbaseData();
                hbaseData.setRowKey(result.getRow());
                ArrayList<HbaseData.HbaseDataDetail> detail = new ArrayList<>();
                for (Cell cell : result.listCells()) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    HbaseData.HbaseDataDetail hbaseDataDetail = new HbaseData.HbaseDataDetail();
                    hbaseDataDetail.setColumnFamily(CellUtil.cloneFamily(cell));
                    hbaseDataDetail.setColumnName(CellUtil.cloneQualifier(cell));
                    hbaseDataDetail.setColumnValue(CellUtil.cloneValue(cell));
                    detail.add(hbaseDataDetail);
                }
                hbaseData.setData(detail);
                list.add(hbaseData);
            }
            return list;
        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            //每次使用完毕需要关闭连接
            if (table != null) {
                try {
                    table.close();
                } catch (Exception e) {
                    System.out.println("关闭table连接失败" + e.toString());
                }
            }
        }
        return null;
    }

    /**
     * 按照过滤器条件查询分页
     */
    public static List<HbaseData> getByFilter(String name, List<HbaseQuery.QueryDetail> list) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(name));
            Scan scan = new Scan();
            //精准设置查询的列族、列，减少数据量的查询和网络IO
            list.forEach(it -> {
                scan.addColumn(it.getColumnFamily(), it.getColumnName());
            });
            //todo psx

        } catch (Exception e) {
            System.out.println(e.toString());
        } finally {
            //每次使用完毕需要关闭连接
            if (table != null) {
                try {
                    table.close();
                } catch (Exception e) {
                    System.out.println("关闭table连接失败" + e.toString());
                }
            }
        }
        return null;
    }

}
