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
package com.littlehotspot.util.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
     */
    public HBaseHelper(Configuration conf) {
        if (conf == null) {
            throw new IllegalArgumentException("The argument[conf] is null");
        }
        try {
            this.conf = HBaseConfiguration.create(conf);
            this.admin = new HBaseAdmin(this.conf);
            System.out.println("The config of HBase is created");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取HBase配置器
     */
    public HBaseHelper() {
        this(new Configuration());
    }

    /**
     * 创建HBase表
     *
     * @param tableName   表名
     * @param colFamilies 列簇
     */
    public void createTable(String tableName, String colFamilies[]) {
        try {
            if (this.admin.tableExists(tableName)) {
                String printMessage = String.format("The table[%s] of HBase already exists", tableName);
                System.out.println(printMessage);
                return;
            }

            HTableDescriptor dsc = new HTableDescriptor(tableName);
            int len = colFamilies.length;
            for (int i = 0; i < len; i++) {
                HColumnDescriptor family = new HColumnDescriptor(colFamilies[i]);
                dsc.addFamily(family);
            }
            admin.createTable(dsc);
            String createTableMessage = String.format("The table[%s] of HBase is created", tableName);
            System.out.println(createTableMessage);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public void deleteTable(String tableName) {
        try {
            if (!this.admin.tableExists(tableName)) {
                String printMessage = String.format("The table[%s] of HBase is not exists", tableName);
                System.out.println(printMessage);
                return;
            }
            admin.disableTable(tableName);
            String disableTableMessage = String.format("The table[%s] of HBase is disabled", tableName);
            System.out.println(disableTableMessage);

            admin.deleteTable(tableName);
            String dropTableMessage = String.format("The table[%s] of HBase is abandoned", tableName);
            System.out.println(dropTableMessage);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
     */
    public void insertCellRecord(String tableName, String rowKey, String family, String column, String value) {
        try {
            HTable table = new HTable(this.conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
            table.put(put);

            String insertCellMessage = String.format("The cell[%s:%s] for '%s' in table[%s] is set to %s", family, column, rowKey, tableName, value);
            System.out.println(insertCellMessage);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
     */
    public void insert(Object object) {
        try {
            Class<?> beanClass = object.getClass();
            HBaseTable hBaseTable = beanClass.getAnnotation(HBaseTable.class);
            String tableName = hBaseTable.name();
            AbstractHelper.Context hBaseContext = new AbstractHelper.Context(tableName);

            AbstractHelper.getTableDataFromBean(hBaseContext, object, beanClass);
            Object rowKeyObject = hBaseContext.getRowKey().getValue();
            if (rowKeyObject == null) {
                return;
            }

            String rowKey = rowKeyObject.toString();
            if (StringUtils.isBlank(rowKey)) {
                return;
            }

            HTable table = new HTable(this.conf, hBaseContext.getTableName());// HTabel负责跟记录相关的操作如增删改查等
            Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
            Collection<AbstractHelper.Column> columns = hBaseContext.getColumnMap().values();
            long version = System.currentTimeMillis();
            for (AbstractHelper.Column entry : columns) {
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 删除一行记录
     *
     * @param tableName 表名
     * @param rowKey    主键
     */
    public void deleteRecord(String tableName, String rowKey) {
        try {
            HTable table = new HTable(this.conf, tableName);
            Delete del = new Delete(Bytes.toBytes(rowKey));
            table.delete(del);

            String deleteRowMessage = String.format("The row for '%s' in table[%s] is deleted", rowKey, tableName);
            System.out.println(deleteRowMessage);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取一条记录
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @return Result
     */
    public Result getOneRecord(String tableName, String rowKey) {
        try {
            HTable table = new HTable(this.conf, tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取所有数据
     *
     * @param tableName 表名
     * @return List
     */
    public List<Result> getAllRecord(String tableName) {
        try {
            HTable table = new HTable(this.conf, tableName);
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            List<Result> list = new ArrayList<>();
            for (Result result : scanner) {
                list.add(result);
            }
            scanner.close();
            return list;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 通过用正则表达式匹配行键，来达到数据的模糊查询。
     *
     * @param tableName   表名
     * @param rowKeyRegex 行键的正则表达式
     * @return List
     */
    public List<Result> searchByRowKeyRegex(String tableName, String rowKeyRegex) {
        try {
            HTable table = new HTable(this.conf, tableName);
            Scan scan = new Scan();
            RegexStringComparator rowKeyRegexStringComparator = new RegexStringComparator(rowKeyRegex);
            Filter rowKeyFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, rowKeyRegexStringComparator);
            scan.setFilter(rowKeyFilter);
            ResultScanner scanner = table.getScanner(scan);
            List<Result> list = new ArrayList<>();
            for (Result result : scanner) {
                list.add(result);
            }
            scanner.close();
            return list;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将对象转换成 HBase {@link Put} 对象
     *
     * @param bean 对象
     * @return {@link Put}
     */
    public static Put toPut(Object bean) {
        try {
            Class<?> beanClass = bean.getClass();
            HBaseTable hBaseTable = beanClass.getAnnotation(HBaseTable.class);
            String tableName = hBaseTable.name();
            AbstractHelper.Context hBaseContext = new AbstractHelper.Context(tableName);

            AbstractHelper.getTableDataFromBean(hBaseContext, bean, beanClass);
            Object rowKeyObject = hBaseContext.getRowKey().getValue();
            if (rowKeyObject == null) {
                return null;
            }

            String rowKey = rowKeyObject.toString();
            if (StringUtils.isBlank(rowKey)) {
                return null;
            }

            Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
            Collection<AbstractHelper.Column> columns = hBaseContext.getColumnMap().values();
            long version = System.currentTimeMillis();
            for (AbstractHelper.Column entry : columns) {
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T toBean(Result result, Class<T> clazz) {
        try {
            byte[] rowKeyBytes = result.getRow();
            String rowKey = Bytes.toString(rowKeyBytes);
            System.out.println(rowKey);

            Cell[] rowCellArray = result.rawCells();
            for (Cell cell : rowCellArray) {
                byte[] rowBytes = cell.getRowArray();
                int rowOffset = cell.getRowOffset();
                short rowLength = cell.getRowLength();
                String row = Bytes.toString(rowBytes, rowOffset, rowLength);
                System.out.print(row + "/");

                byte[] familyBytes = cell.getFamilyArray();
                int familyOffset = cell.getFamilyOffset();
                byte familyLength = cell.getFamilyLength();
                String family = Bytes.toString(familyBytes, familyOffset, familyLength);
                System.out.print(family + ":");

                byte[] qualifierBytes = cell.getQualifierArray();
                int qualifierOffset = cell.getQualifierOffset();
                int qualifierLength = cell.getQualifierLength();
                String qualifier = Bytes.toString(qualifierBytes, qualifierOffset, qualifierLength);
                System.out.print(qualifier + "=");

                byte[] valueBytes = cell.getValueArray();
                int valueOffset = cell.getValueOffset();
                int valueLength = cell.getValueLength();
                String value = Bytes.toString(valueBytes, valueOffset, valueLength);
                System.out.println(value);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
