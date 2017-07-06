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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.StringUtils;
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
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
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


//        Map<String, Object> columnMap = new ConcurrentHashMap<>();
        Map<String, Column> columnMap = new HashMap<>();

        Object rowKeyObjectFromFields = this.getDataFromFields(columnMap, object, beanClass);
        Object rowKeyObjectFromMethods = this.getDataFromMethods(columnMap, object, beanClass);
        Object rowKeyObject = rowKeyObjectFromFields == null ? rowKeyObjectFromMethods : rowKeyObjectFromFields;
        if (rowKeyObject == null) {
            return;
        }

        String rowKey = rowKeyObject.toString();
        if (rowKey == null || rowKey.trim().isEmpty()) {
            return;
        }

        HTable table = new HTable(this.conf, tableName);// HTabel负责跟记录相关的操作如增删改查等
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        Collection<Column> columns = columnMap.values();
        long version = System.currentTimeMillis();
        for (Column entry : columns) {
            String familyName = entry.getFamilyName();
            String columnName = entry.getColumnName();
            Object valueObject = entry.getColumnValue();
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
        Delete del = new Delete(Bytes.toBytes(rowKey));
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
        Get get = new Get(Bytes.toBytes(rowKey));
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
        for (Result result : scanner) {
            list.add(result);
        }
        scanner.close();
        return list;
    }

    // 从方法中获取数据
    private Object getDataFromMethods(Map<String, Column> columnMap, Object bean, Class<?> beanClass) throws IllegalAccessException, InvocationTargetException {
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
            if (hBaseRowKey != null) {
                rowKeyObject = method.invoke(bean);
                continue;
            }
            HBaseColumn hBaseColumn = method.getAnnotation(HBaseColumn.class);
            if (hBaseColumn == null) {
                continue;
            }
            String familyName = hBaseColumn.familyName();
            if (StringUtils.isBlank(familyName)) {
                continue;
            }
            String columnName = hBaseColumn.columnName();
            if (columnMap.get(columnName) != null) {
                continue;
            }
            Object result = method.invoke(bean);

            String key = String.format("f=%s|c=%s", familyName, columnName);
            Column hBaseColumnValue = new Column(familyName, columnName, result);

            columnMap.put(key, hBaseColumnValue);
        }
        Class<?> superclass = beanClass.getSuperclass();
        Object rowKeyObjectFromSub = null;
        if (superclass != null) {
            rowKeyObjectFromSub = this.getDataFromMethods(columnMap, bean, superclass);
        }
        return rowKeyObject == null ? rowKeyObjectFromSub : rowKeyObject;
    }

    // 从属性中获取数据
    private Object getDataFromFields(Map<String, Column> columnMap, Object bean, Class<?> beanClass) throws IllegalAccessException {
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
            if (hBaseRowKey != null) {
                rowKeyObject = this.getFieldValue(bean, field);
                continue;
            }
            HBaseColumn hBaseColumn = field.getAnnotation(HBaseColumn.class);
            if (hBaseColumn == null) {
                continue;
            }
            String familyName = hBaseColumn.familyName();
            if (StringUtils.isBlank(familyName)) {
                continue;
            }
            String columnName = hBaseColumn.columnName();
            if (columnMap.get(columnName) != null) {
                continue;
            }
            Object result = this.getFieldValue(bean, field);

            String key = String.format("f=%s|c=%s", familyName, columnName);
            Column hBaseColumnValue = new Column(familyName, columnName, result);

            columnMap.put(key, hBaseColumnValue);
        }
        Class<?> superclass = beanClass.getSuperclass();
        Object rowKeyObjectFromSub = null;
        if (superclass != null) {
            rowKeyObjectFromSub = this.getDataFromFields(columnMap, bean, superclass);
        }
        return rowKeyObject == null ? rowKeyObjectFromSub : rowKeyObject;
    }

    // 设置属性权限
    private Object getFieldValue(Object bean, Field field) throws IllegalAccessException {
        field.setAccessible(true);
        return field.get(bean);
    }

    /**
     * 上下文
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    protected class Column {

        /**
         * 列族名
         */
        private String familyName;

        /**
         * 列名
         */
        private String columnName;

        /**
         * 列值
         */
        private Object columnValue;
    }
}
