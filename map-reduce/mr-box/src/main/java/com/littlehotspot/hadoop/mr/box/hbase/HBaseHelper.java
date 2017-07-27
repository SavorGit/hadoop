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
package com.littlehotspot.hadoop.mr.box.hbase;

import lombok.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private Connection connection;

    /**
     * 获取HBase配置器
     *
     * @param conf Hadoop配置器
     * @throws IOException 异常
     */
    public HBaseHelper(Configuration conf) throws IOException {
        this.conf = HBaseConfiguration.create(conf);
        this.admin = new HBaseAdmin(this.conf);
        this.connection=ConnectionFactory.createConnection(conf);
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
        String tableName = hBaseTable.name();
        Context hBaseContext = new Context(tableName);

        this.analysisHBaseData(object, beanClass, hBaseContext);
        Object rowKeyObject = hBaseContext.getRowKey().getValue();
        if (rowKeyObject == null) {
            return;
        }

        String rowKey = rowKeyObject.toString();
        if (StringUtils.isBlank(rowKey)) {
            return;
        }

//        HTable table = new HTable(this.conf, hBaseContext.getTableName());// HTabel负责跟记录相关的操作如增删改查等
        Table table =connection.getTable(TableName.valueOf(hBaseContext.getTableName()));// HTabel负责跟记录相关的操作如增删改查等
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        Collection<Column> columns = hBaseContext.getColumnMap().values();
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
        table.close();
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

    public Put getPut(Object object) throws InvocationTargetException, IllegalAccessException {
        Class<?> beanClass = object.getClass();
        HBaseTable hBaseTable = beanClass.getAnnotation(HBaseTable.class);
        String tableName = hBaseTable.name();
        Context hBaseContext = new Context(tableName);

        this.analysisHBaseData(object, beanClass, hBaseContext);
        Object rowKeyObject = hBaseContext.getRowKey().getValue();
        if (rowKeyObject == null) {
            return null;
        }

        String rowKey = rowKeyObject.toString();
        if (StringUtils.isBlank(rowKey)) {
            return null;
        }
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        Collection<Column> columns = hBaseContext.getColumnMap().values();
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
        return put;
    }

    // 解析 HBase 数据
    private void analysisHBaseData(Object object, Class<?> beanClass, Context hBaseContext) throws IllegalAccessException, InvocationTargetException {
        this.getDataFromTableFields(hBaseContext, object, beanClass);// 处理字段
        this.getDataFromMethods(hBaseContext, object, beanClass);// 处理方法
    }

    // 从方法中获取数据
    private void getDataFromMethods(Context context, Object bean, Class<?> beanClass) throws IllegalAccessException, InvocationTargetException {
        if (context == null) {
            throw new IllegalArgumentException("HBase-Helper argument 'context' from fields is null");
        }
        if (bean == null) {
            throw new IllegalArgumentException("HBase-Helper argument 'bean' from fields is null");
        }

        // 准备工作
        if (context.getColumnMap() == null) {
            throw new IllegalStateException("The column-map from Family-Fields-Context is null");
        }
        if (beanClass == null) {
            beanClass = bean.getClass();
        }

        Method[] methods = beanClass.getDeclaredMethods();
        for (Method method : methods) {

            // RowKey 处理
            HBaseRowKey hBaseRowKeyAnnotation = method.getAnnotation(HBaseRowKey.class);
            if (hBaseRowKeyAnnotation != null) {
                if (context.getRowKey() == null) {
                    context.setRowKey(new RowKey());
                }
                if (context.getRowKey().getValue() == null) {
                    Object rowKeyObject = method.invoke(bean);
                    context.getRowKey().setValue(rowKeyObject);
                }
                continue;
            }

            // 获取列族名
            HBaseFamily hBaseFamilyAnnotation = method.getAnnotation(HBaseFamily.class);
            if (hBaseFamilyAnnotation == null) {
                continue;
            }
            String familyName = hBaseFamilyAnnotation.name();
            if (StringUtils.isBlank(familyName)) {
                continue;
            }
            context.setFamilyName(familyName);

            // 获取列名
            HBaseColumn hBaseColumnAnnotation = method.getAnnotation(HBaseColumn.class);
            if (hBaseColumnAnnotation == null) {
                continue;
            }
            String columnName = hBaseColumnAnnotation.name();
            if (StringUtils.isBlank(columnName)) {
                continue;
            }

            // 已经有值时退出本次循环
            if (context.getColumnMap().get(columnName) != null) {
                continue;
            }
            Object methodResult = method.invoke(bean);

            String key = String.format("f=%s|c=%s", familyName, columnName);
            Column hBaseColumnValue = new Column(familyName, columnName, methodResult);

            context.getColumnMap().put(key, hBaseColumnValue);
        }
        Class<?> superclass = beanClass.getSuperclass();
        if (superclass != null) {
            this.getDataFromMethods(context, bean, superclass);
        }
    }

    // 从属性中获取数据 - 表对象
    private void getDataFromTableFields(Context context, Object bean, Class<?> beanClass) throws IllegalAccessException {
        if (context == null) {
            throw new IllegalArgumentException("HBase-Helper argument 'context' from fields is null");
        }
        if (bean == null) {
            throw new IllegalArgumentException("HBase-Helper argument 'bean' from fields is null");
        }

        // 准备工作
        if (context.getColumnMap() == null) {
            throw new IllegalStateException("The column-map from Family-Fields-Context is null");
        }
        if (beanClass == null) {
            beanClass = bean.getClass();
        }

        Field[] fields = beanClass.getDeclaredFields();
        for (Field field : fields) {

            // RowKey 处理
            HBaseRowKey hBaseRowKeyAnnotation = field.getAnnotation(HBaseRowKey.class);
            if (hBaseRowKeyAnnotation != null) {
                if (context.getRowKey() == null) {
                    context.setRowKey(new RowKey());
                }
                if (context.getRowKey().getValue() == null) {
                    Object rowKeyObject = this.getFieldValue(bean, field);
                    context.getRowKey().setValue(rowKeyObject);
                }
                continue;
            }

            // 获取列族名
            HBaseFamily hBaseFamilyAnnotation = field.getAnnotation(HBaseFamily.class);
            if (hBaseFamilyAnnotation == null) {
                continue;
            }
            String familyName = hBaseFamilyAnnotation.name();
            if (StringUtils.isBlank(familyName)) {
                continue;
            }
            context.setFamilyName(familyName);

            // 获取列名
            HBaseColumn hBaseColumnAnnotation = field.getAnnotation(HBaseColumn.class);
            if (hBaseColumnAnnotation == null) {
                Object family = this.getFieldValue(bean, field);
                this.getDataFromFamilyFields(context, family, null);
                continue;
            }
            String columnName = hBaseColumnAnnotation.name();
            if (StringUtils.isBlank(columnName)) {
                continue;
            }

            // 已经有值时退出本次循环
            if (context.getColumnMap().get(columnName) != null) {
                continue;
            }

            Object fieldValue = this.getFieldValue(bean, field);

            String key = String.format("f=%s|c=%s", familyName, columnName);
            Column hBaseColumnValue = new Column(familyName, columnName, fieldValue);

            context.getColumnMap().put(key, hBaseColumnValue);
        }
        Class<?> superclass = beanClass.getSuperclass();
        if (superclass != null) {
            this.getDataFromTableFields(context, bean, superclass);
        }
    }

    // 从属性中获取数据 - 列族对象
    private void getDataFromFamilyFields(Context context, Object bean, Class<?> beanClass) throws IllegalAccessException {
        if (context == null) {
            throw new IllegalArgumentException("The argument 'context' from Family-Fields is null");
        }
        if (bean == null) {
            throw new IllegalArgumentException("The argument 'bean' from Family-Fields is null");
        }

        // 准备工作
        String familyName = context.getFamilyName();
        if (StringUtils.isBlank(familyName)) {
            throw new IllegalStateException("The family-name from Family-Fields-Context is null");
        }
        if (context.getColumnMap() == null) {
            throw new IllegalStateException("The column-map from Family-Fields-Context is null");
        }
        if (beanClass == null) {
            beanClass = bean.getClass();
        }

        Field[] fields = beanClass.getDeclaredFields();
        for (Field field : fields) {

            // 获取列名
            HBaseColumn hBaseColumnAnnotation = field.getAnnotation(HBaseColumn.class);
            if (hBaseColumnAnnotation == null) {
                continue;
            }
            String columnName = hBaseColumnAnnotation.name();
            if (StringUtils.isBlank(columnName)) {
                continue;
            }

            // 已经有值时退出本次循环
            if (context.getColumnMap().get(columnName) != null) {
                continue;
            }

            Object fieldValue = this.getFieldValue(bean, field);

            String key = String.format("f=%s|c=%s", familyName, columnName);
            Column hBaseColumnValue = new Column(familyName, columnName, fieldValue);

            context.getColumnMap().put(key, hBaseColumnValue);
        }
        Class<?> superclass = beanClass.getSuperclass();
        if (superclass != null) {
            this.getDataFromFamilyFields(context, bean, superclass);
        }
    }

    // 设置属性权限
    private Object getFieldValue(Object bean, Field field) throws IllegalAccessException {
        field.setAccessible(true);
        return field.get(bean);
    }

    public void closeConnection(){
        if(this.connection!=null){
            try {
                this.connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 上下文
     */
    private class Context {

        /**
         * 表名
         */
        @Setter
        @Getter
        private String tableName;

        /**
         * 列族名
         */
        @Setter
        @Getter
        private String familyName;

        /**
         * 行键
         */
        @Setter
        @Getter
        private RowKey rowKey;

        /**
         * 列字典
         */
        @Getter
        Map<String, Column> columnMap;

        /**
         * 无参构造
         */
        public Context() {
            super();
            this.columnMap = new ConcurrentHashMap<>();
            this.rowKey = new RowKey();
        }

        public Context(String tableName) {
            this();
            this.tableName = tableName;
        }
    }

    /**
     * 行键
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private class RowKey {

        private Object value;
    }

    /**
     * 列对象
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private class Column {

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
