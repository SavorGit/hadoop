/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hbase.io
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 09:56
 */
package com.littlehotspot.hadoop.mr.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年08月04日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public abstract class AbstractWritable {

    protected static final char FIELD_DELIMITER = 0x0001;

    protected void setValue(Class<?> clazz, Object bean, String fieldName, ResultSet result, String mysqlColumnName) throws SQLException {
        try {
            int columnIndex = result.findColumn(mysqlColumnName);
            ResultSetMetaData resultSetMetaData = result.getMetaData();
//            String ColumnClassName = resultSetMetaData.getColumnClassName(columnIndex);
            String ColumnTypeName = resultSetMetaData.getColumnTypeName(columnIndex);
            Object value;
            if ("TINYINT".equalsIgnoreCase(ColumnTypeName)) {
                value = result.getInt(columnIndex);
            } else if ("TIMESTAMP".equalsIgnoreCase(ColumnTypeName)) {
                value = result.getTimestamp(columnIndex).getTime();
            } else if ("DATETIME".equalsIgnoreCase(ColumnTypeName)) {
                value = result.getTimestamp(columnIndex).getTime();
            } else if ("DATE".equalsIgnoreCase(ColumnTypeName)) {
                value = result.getDate(columnIndex).getTime();
            } else if ("TIME".equalsIgnoreCase(ColumnTypeName)) {
                value = result.getTime(columnIndex).getTime();
            } else {
                value = result.getObject(columnIndex);
            }
            Field field = clazz.getDeclaredField(fieldName);
            boolean accessible = field.isAccessible();
            field.setAccessible(true);
            field.set(bean, value);
            field.setAccessible(accessible);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void addColumn(Class<?> clazz, Object bean, String fieldName, Put put, String familyName, String hBaseColumnName, long version) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            boolean accessible = field.isAccessible();
            field.setAccessible(true);
            Object value = field.get(bean);
            field.setAccessible(accessible);
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(hBaseColumnName), version, Bytes.toBytes(String.valueOf(value)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
