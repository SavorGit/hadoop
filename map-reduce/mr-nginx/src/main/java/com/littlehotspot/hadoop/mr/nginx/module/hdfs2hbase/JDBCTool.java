/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 14:30
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase;

import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年07月14日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class JDBCTool {

    // 表示定义数据库的用户名
    private String username;

    // 定义数据库的密码
    private String password;

    // 定义数据库的驱动信息
    private String driver;

    // 定义访问数据库的地址
    private String url;


    // 定义数据库的链接
    private Connection connection;

    // 定义sql语句的执行对象
    private PreparedStatement preparedStatement;

    // 定义查询返回的结果集合
    private ResultSet resultSet;

    public JDBCTool(String driver, String url, String username, String password) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    /**
     * 获取数据库连接
     *
     * @return 数据库连接
     */
    public Connection getConnection() {
        try {
            Class.forName(this.driver); // 注册驱动
            this.connection = DriverManager.getConnection(this.url, this.username, this.password); // 获取连接
        } catch (Exception e) {
            throw new RuntimeException("get connection error", e);
        }
        return this.connection;
    }

    /**
     * 执行更新操作
     *
     * @param sql    sql语句
     * @param params 执行参数
     * @return 执行结果
     * @throws SQLException
     */
    public boolean updateByPreparedStatement(String sql, List<?> params) throws SQLException {
        boolean flag = false;
        int result = -1;// 表示当用户执行添加删除和修改的时候所影响数据库的行数
        this.preparedStatement = this.connection.prepareStatement(sql);
        int index = 1;
        // 填充sql语句中的占位符
        if (params != null && !params.isEmpty()) {
            for (int i = 0; i < params.size(); i++) {
                this.preparedStatement.setObject(index++, params.get(i));
            }
        }
        result = this.preparedStatement.executeUpdate();
        flag = result > 0 ? true : false;
        return flag;
    }

    public ResultSet executeSQL(String sql, Object... params) throws SQLException {
        if (sql == null) {
            throw new IllegalArgumentException("The argument[sql] is null");
        }
        if (sql.trim().length() < 1) {
            String errorMessage = String.format("The argument[sql] is \"%s\"", sql);
            throw new IllegalArgumentException(errorMessage);
        }
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        this.preparedStatement = connection.prepareStatement(sql);
        if (params != null && params.length > 0) {
            for (int index = 0, argIndex = 1; index < params.length; index++) {
                this.preparedStatement.setObject(argIndex++, params[index]);
            }
        }
        this.resultSet = this.preparedStatement.executeQuery();
        return this.resultSet;
    }

    /**
     * 执行查询操作
     *
     * @param sql    sql语句
     * @param params 执行参数
     * @return List
     * @throws SQLException Sql 异常
     */
    public List<Map<String, Object>> findResult(String sql, Object... params) throws SQLException {
        ResultSet resultSet = this.executeSQL(sql, params);
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<String, Object>();
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                String columnName = resultSetMetaData.getColumnName(columnIndex + 1);
                if (columnName == null || columnName.trim().length() < 1) {
                    continue;
                }
                Object columnValue = resultSet.getObject(columnName);
                map.put(columnName, columnValue);
            }
            list.add(map);
        }
        return list;
    }

    /**
     * 执行查询操作
     *
     * @param sql    sql语句
     * @param params 执行参数
     * @return List
     * @throws SQLException Sql 异常
     */
    public <T> List<T> findResult(Class<T> clazz, String sql, Object... params) throws SQLException {
        if (sql == null) {
            throw new IllegalArgumentException("The argument[sql] is null");
        }
        if (sql.trim().length() < 1) {
            String errorMessage = String.format("The argument[sql] is \"%s\"", sql);
            throw new IllegalArgumentException(errorMessage);
        }
        return findResult(this.connection, clazz, sql, params);
    }

    /**
     * 执行查询操作
     *
     * @param connection 连接对象
     * @param clazz      JAVA 对象的类型
     * @param sql        SQL 语句
     * @param params     SQL 语句参数
     * @param <T>        java 泛型
     * @return List
     * @throws SQLException SQL 异常
     */
    public <T> List<T> findResult(Connection connection, Class<T> clazz, String sql, Object... params) throws SQLException {
        if (sql == null) {
            throw new IllegalArgumentException("The argument[sql] is null");
        }
        if (sql.trim().length() < 1) {
            String errorMessage = String.format("The argument[sql] is \"%s\"", sql);
            throw new IllegalArgumentException(errorMessage);
        }
        List<T> list = new ArrayList<T>();
        this.preparedStatement = connection.prepareStatement(sql);
        if (params != null && params.length > 0) {
            for (int index = 0, argIndex = 1; index < params.length; index++) {
                this.preparedStatement.setObject(argIndex++, params[index]);
            }
        }
        return findResult(this.preparedStatement, clazz);
    }

    /**
     * 执行查询操作
     *
     * @param preparedStatement SQL 语句预处理器
     * @param clazz             JAVA 对象的类型
     * @return List
     * @throws SQLException Sql 异常
     */
    public <T> List<T> findResult(PreparedStatement preparedStatement, Class<T> clazz) throws SQLException {
        if (preparedStatement == null) {
            throw new IllegalArgumentException("The argument[preparedStatement] is null");
        }
        if (clazz == null) {
            throw new IllegalArgumentException("The argument[clazz] is null");
        }
        this.resultSet = preparedStatement.executeQuery();
        return toList(this.resultSet, clazz);
    }

    /**
     * 将 {@link ResultSet} 对象转换为 JAVA 对象
     *
     * @param resultSet {@link ResultSet} 对象
     * @param clazz     JAVA 对象的类型
     * @param <T>       java 泛型
     * @return List
     * @throws SQLException SQL 异常
     */
    public <T> List<T> toList(ResultSet resultSet, Class<T> clazz) throws SQLException {
        if (resultSet == null) {
            throw new IllegalArgumentException("The argument[resultSet] is null");
        }
        if (clazz == null) {
            throw new IllegalArgumentException("The argument[clazz] is null");
        }
        List<T> list = new ArrayList<T>();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        while (resultSet.next()) {
            try {
                T bean = clazz.newInstance();
                for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
                    try {
                        String columnName = resultSetMetaData.getColumnName(columnIndex + 1);
                        if (columnName == null || columnName.trim().length() < 1) {
                            continue;
                        }
                        Field field = this.getField(clazz, columnName);
                        if (field == null) {
                            continue;
                        }
                        String methodName = String.format("set%s", StringUtils.capitalize(columnName));
                        Method method = this.getFieldSetMethod(clazz, field, methodName);
                        if (method == null) {
                            continue;
                        }
                        this.executeMethod(bean, columnName, field, method);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                list.add(bean);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    /**
     * 释放资源
     */
    public void releaseConnection() {
        this.close(resultSet);
        this.close(preparedStatement);
        this.close(connection);
    }

    // 释放资源
    private void close(AutoCloseable autoCloseable) {
        if (autoCloseable != null) {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                autoCloseable = null;
            }
        }
    }

    // 获取属性的 Set 方法
    private Method getFieldSetMethod(Class<?> clazz, Field field, String methodName) throws NoSuchMethodException {
        Class<?> fieldType = field.getType();
        Method method = clazz.getMethod(methodName, fieldType);
        if (method == null) {
            method = this.getFieldSetMethod(clazz, field, methodName);
        }
        return method;
    }

    // 获取属性
    private Field getField(Class<?> clazz, String columnName) throws NoSuchFieldException {
        Field field = clazz.getDeclaredField(columnName);
        if (field == null) {
            field = this.getField(clazz.getSuperclass(), columnName);
        }
        return field;
    }

    // 执行方法
    private <T> void executeMethod(T bean, String columnName, Field field, Method method) throws SQLException, IllegalAccessException, InvocationTargetException {
//        if (byte.class.equals(field.getType()) || Byte.class.equals(field.getType())) {
//            Byte columnValue = (Byte) resultSet.getObject(columnName);
//            method.invoke(bean, columnValue);
//        } else if (short.class.equals(field.getType()) || Short.class.equals(field.getType())) {
//            Short columnValue = (Short) resultSet.getObject(columnName);
//            method.invoke(bean, columnValue);
//        } else if (int.class.equals(field.getType()) || Integer.class.equals(field.getType())) {
//            Integer columnValue = (Integer) resultSet.getObject(columnName);
//            method.invoke(bean, columnValue);
//        } else if (long.class.equals(field.getType()) || Long.class.equals(field.getType())) {
//            Long columnValue = (Long) resultSet.getObject(columnName);
//            method.invoke(bean, columnValue);
//        } else if (float.class.equals(field.getType()) || Float.class.equals(field.getType())) {
//            Float columnValue = (Float) resultSet.getObject(columnName);
//            method.invoke(bean, columnValue);
//        } else if (double.class.equals(field.getType()) || Double.class.equals(field.getType())) {
//            Double columnValue = (Double) resultSet.getObject(columnName);
//            method.invoke(bean, columnValue);
//        } else if (char.class.equals(field.getType()) || Character.class.equals(field.getType())) {
//            Character columnValue = (Character) resultSet.getObject(columnName);
//            method.invoke(bean, columnValue);
//        } else if (boolean.class.equals(field.getType()) || Boolean.class.equals(field.getType())) {
//            Boolean columnValue = (Boolean) resultSet.getObject(columnName);
//            method.invoke(bean, columnValue);
//        } else {
        Object columnValue = resultSet.getObject(columnName);
        method.invoke(bean, columnValue);
//        }
    }
}
