/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:49
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.HBaseTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * <h1>工具 - HBase</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月02日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class HBaseHelper {
    private Configuration conf;// 配置器
    private HBaseAdmin admin;// HBase管理员

    /**
     * 获取HBase配置器
     *
     * @param conf Hadoop配置器
     * @throws IOException 异常
     */
    public HBaseHelper(Configuration conf) throws IOException {
        this.conf = HBaseConfiguration.create(conf);
        this.admin = new HBaseAdmin(this.conf);
        System.out.println("创建 HBase 配置成功！");
    }

    /**
     * 获取HBase配置器
     *
     * @throws IOException 异常
     */
    public HBaseHelper() throws IOException {
        this(new Configuration());
    }

    /**
     * 创建HBase表
     *
     * @param tableName   表名
     * @param colFamilies 列簇
     * @throws IOException 异常
     */
    public void createTable(String tableName, String colFamilies[]) throws IOException {
        if (this.admin.tableExists(tableName)) {
            System.out.println("Table: " + tableName + " already exists !");
        } else {
            HTableDescriptor dsc = new HTableDescriptor(tableName);
            int len = colFamilies.length;
            for (int i = 0; i < len; i++) {
                HColumnDescriptor family = new HColumnDescriptor(colFamilies[i]);
                dsc.addFamily(family);
            }
            admin.createTable(dsc);
            System.out.println("创建表" + tableName + "成功");
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     * @throws IOException 异常
     */
    public void deleteTable(String tableName) throws IOException {
        if (this.admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            System.out.println("禁用表" + tableName + "!");
            admin.deleteTable(tableName);
            System.out.println("删除表成功!");
        } else {
            System.out.println(tableName + "表不存在 !");
        }
    }

    /**
     * 插入记录
     *
     * @param tableName 表名
     * @param rowKey    键
     * @param family    簇
     * @param column    列
     * @param value     值
     * @throws IOException 异常
     */
    public void insertRecord(String tableName, String rowKey, String family, String column, String value) throws IOException {
        HTable table = new HTable(this.conf, tableName);
        Put put = new Put(rowKey.getBytes());
        put.add(family.getBytes(), column.getBytes(), value.getBytes());
        table.put(put);
        System.out.println(tableName + "插入key:" + rowKey + "行成功!");
    }

//    /**
//     * 为表添加数据（适合知道有多少列族的固定表）
//     *
//     * @param rowKey    rowKey
//     * @param tableName 表名
//     * @param column1   第一个列族列表
//     * @param value1    第一个列的值的列表
//     */
//    private void addData(String rowKey, String tableName, String[] column1, String[] value1) throws IOException {
//        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
//        HTable table = new HTable(this.conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
//        // 获取表
//        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
//                .getColumnFamilies();
//
//        for (int i = 0; i < columnFamilies.length; i++) {
//            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
//            if (familyName.equals("article")) { // article列族put数据
//                for (int j = 0; j < column1.length; j++) {
//                    put.add(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
//                }
//            }
//
//        }
//        table.put(put);
//        System.out.println("add data Success!");
//    }

    /**
     * 输入对象到 HBase
     *
     * @param object 对象
     * @throws InvocationTargetException 异常
     * @throws IllegalAccessException    异常
     * @throws IOException               异常
     */
    public void insert(Object object) throws InvocationTargetException, IllegalAccessException, IOException {
        Class<?> beanClass = object.getClass();
        HBaseTable hBaseTable = beanClass.getAnnotation(HBaseTable.class);
        String tableName = hBaseTable.tableName();
        String familyName = hBaseTable.familyName();


//        Map<String, Object> columnMap = new ConcurrentHashMap<>();
        Map<String, Object> columnMap = new HashMap<>();

        Object rowKeyObjectFromFields = this.getDataFromFields(object, beanClass, columnMap);
        Object rowKeyObjectFromMethods = this.getDataFromMethods(object, beanClass, columnMap);
        Object rowKeyObject = rowKeyObjectFromFields == null ? rowKeyObjectFromMethods : rowKeyObjectFromFields;
        if (rowKeyObject == null) {
            return;
        }

        String rowKey = rowKeyObject.toString();
        HTable table = new HTable(this.conf, tableName);// HTabel负责跟记录相关的操作如增删改查等
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        Set<Map.Entry<String, Object>> entrySet = columnMap.entrySet();
        long version = System.currentTimeMillis();
        for (Map.Entry<String, Object> entry : entrySet) {
            String columnName = entry.getKey();
            Object valueObject = entry.getValue();
            String value = "";
            if (valueObject != null) {
                value = valueObject.toString();
            }
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), version, Bytes.toBytes(value));
        }
        table.put(put);
    }

    /**
     * 删除一行记录
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @throws IOException 异常
     */
    public void deleteRecord(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(this.conf, tableName);
        Delete del = new Delete(rowKey.getBytes());
        table.delete(del);
        System.out.println(tableName + "删除行" + rowKey + "成功!");
    }

    /**
     * 获取一条记录
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @return Result
     * @throws IOException 异常
     */
    public Result getOneRecord(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(this.conf, tableName);
        Get get = new Get(rowKey.getBytes());
        return table.get(get);
    }

    /**
     * 获取所有数据
     *
     * @param tableName 表名
     * @return List
     * @throws IOException 异常
     */
    public List<Result> getAllRecord(String tableName) throws IOException {
        HTable table = new HTable(this.conf, tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        List<Result> list = new ArrayList<>();
        for (Result r : scanner) {
            list.add(r);
        }
        scanner.close();
        return list;
    }

    private Object getDataFromMethods(Object bean, Class<?> beanClass, Map<String, Object> columnMap) throws IllegalAccessException, InvocationTargetException {
        if (bean == null || beanClass == null) {
            return null;
        }
        if (columnMap == null) {
//            columnMap = new ConcurrentHashMap<>();
            columnMap = new HashMap<>();
        }
        Object rowKeyObject = null;
        Method[] methods = beanClass.getDeclaredMethods();
        for (Method method : methods) {
            HBaseRowKey hBaseRowKey = method.getAnnotation(HBaseRowKey.class);
            HBaseColumn hBaseColumn = method.getAnnotation(HBaseColumn.class);
            if (hBaseRowKey != null) {
                rowKeyObject = method.invoke(bean);
            }
            if (hBaseColumn == null) {
                continue;
            }
            String columnName = hBaseColumn.columnName();
            if (columnMap.get(columnName) != null) {
                continue;
            }
            Object result = method.invoke(bean);
            columnMap.put(columnName, result);
        }
        Class<?> superclass = beanClass.getSuperclass();
        Object rowKeyObjectFromSub = getDataFromFields(bean, superclass, columnMap);
        return rowKeyObject == null ? rowKeyObjectFromSub : rowKeyObject;
    }

    private Object getDataFromFields(Object bean, Class<?> beanClass, Map<String, Object> columnMap) throws IllegalAccessException {
        if (bean == null || beanClass == null) {
            return null;
        }
        if (columnMap == null) {
//            columnMap = new ConcurrentHashMap<>();
            columnMap = new HashMap<>();
        }
        Object rowKeyObject = null;
        Field[] fields = beanClass.getDeclaredFields();
        for (Field field : fields) {
            HBaseRowKey hBaseRowKey = field.getAnnotation(HBaseRowKey.class);
            HBaseColumn hBaseColumn = field.getAnnotation(HBaseColumn.class);
            if (hBaseRowKey != null) {
                rowKeyObject = this.getFieldValue(bean, field);
            }
            if (hBaseColumn == null) {
                continue;
            }
            String columnName = hBaseColumn.columnName();
            if (columnMap.get(columnName) != null) {
                continue;
            }
            Object result = this.getFieldValue(bean, field);

            columnMap.put(columnName, result);
        }
        Class<?> superclass = beanClass.getSuperclass();
        Object rowKeyObjectFromSub = getDataFromFields(bean, superclass, columnMap);
        return rowKeyObject == null ? rowKeyObjectFromSub : rowKeyObject;
    }

    private Object getFieldValue(Object bean, Field field) throws IllegalAccessException {
        field.setAccessible(true);
        return field.get(bean);
    }


}
