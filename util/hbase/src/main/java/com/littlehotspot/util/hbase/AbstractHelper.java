/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.util.hbase
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 13:53
 */
package com.littlehotspot.util.hbase;

import lombok.*;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <h1>帮手 - 抽象</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年07月21日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public abstract class AbstractHelper {

    protected static final String MAP_KEY_FORMAT_HBASE_COLUMN = "f=%s|c=%s";

    // 从 JAVA 对象获取 HBase 数据
    protected static void getTableDataFromBean(Context context, Object bean, Class<?> beanClass) throws IllegalAccessException, InvocationTargetException {
        getFamilyDataFromBeanFields(context, bean, beanClass);// 处理字段
        getColumnDataFromBeanMethods(context, bean, beanClass);// 处理方法
    }

    // 从数据列表创建 JAVA 对象
    protected static <T> T getBeanFromTableData(Context context, Class<T> beanClass) throws IllegalAccessException, InstantiationException, InvocationTargetException {
        T bean = beanClass.newInstance();
        setBeanFieldsFromFamilyData(context, bean, beanClass);
        setBeanMethodsFromColumnData(context, bean, beanClass);
        return bean;
    }

    /* ============================= 以下是私有方法 ============================= */
    // 从列族数据为对象方法赋值
    private static void setBeanMethodsFromColumnData(Context context, Object bean, Class<?> beanClass) throws IllegalAccessException, InvocationTargetException {
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
                HBaseAnnotationScope hBaseRowKeyAnnotationScope = hBaseRowKeyAnnotation.scope();
                if (HBaseAnnotationScope.SETTER != hBaseRowKeyAnnotationScope) {
                    continue;
                }
                if (context.getRowKey() == null) {
                    continue;
                }
                if (context.getRowKey().getValue() == null) {
                    continue;
                }
                method.invoke(bean, context.getRowKey().getValue());
                context.getRowKey().setValue(null);
                continue;
            }

            // 获取列族名
            HBaseFamily hBaseFamilyAnnotation = method.getAnnotation(HBaseFamily.class);
            if (hBaseFamilyAnnotation == null) {
                continue;
            }
            HBaseAnnotationScope hBaseFamilyAnnotationScope = hBaseFamilyAnnotation.scope();
            if (HBaseAnnotationScope.SETTER != hBaseFamilyAnnotationScope) {
                continue;
            }
            String familyName = hBaseFamilyAnnotation.name();
            if (StringUtils.isBlank(familyName)) {
                continue;
            }

            // 获取列名
            HBaseColumn hBaseColumnAnnotation = method.getAnnotation(HBaseColumn.class);
            if (hBaseColumnAnnotation == null) {
                continue;
            }
            HBaseAnnotationScope hBaseColumnAnnotationScope = hBaseColumnAnnotation.scope();
            if (HBaseAnnotationScope.SETTER != hBaseColumnAnnotationScope) {
                continue;
            }
            String columnName = hBaseColumnAnnotation.name();
            if (StringUtils.isBlank(columnName)) {
                continue;
            }

            // 已经有值时退出本次循环
            String key = String.format(MAP_KEY_FORMAT_HBASE_COLUMN, familyName, columnName);
            Column column = context.getColumnMap().get(key);
            if (column == null) {
                continue;
            }

            method.invoke(bean, column.getColumnValue());
        }

        Class<?> superclass = beanClass.getSuperclass();
        if (superclass == null) {
            return;
        }
        setBeanMethodsFromColumnData(context, bean, superclass);
    }

    // 从方法中获取数据
    private static void getColumnDataFromBeanMethods(Context context, Object bean, Class<?> beanClass) throws IllegalAccessException, InvocationTargetException {
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
                HBaseAnnotationScope hBaseRowKeyAnnotationScope = hBaseRowKeyAnnotation.scope();
                if (HBaseAnnotationScope.GETTER != hBaseRowKeyAnnotationScope) {
                    continue;
                }
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
            HBaseAnnotationScope hBaseFamilyAnnotationScope = hBaseFamilyAnnotation.scope();
            if (HBaseAnnotationScope.GETTER != hBaseFamilyAnnotationScope) {
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
            HBaseAnnotationScope hBaseColumnAnnotationScope = hBaseColumnAnnotation.scope();
            if (HBaseAnnotationScope.GETTER != hBaseColumnAnnotationScope) {
                continue;
            }
            String columnName = hBaseColumnAnnotation.name();
            if (StringUtils.isBlank(columnName)) {
                continue;
            }

            // 已经有值时退出本次循环
            String key = String.format(MAP_KEY_FORMAT_HBASE_COLUMN, familyName, columnName);
            if (context.getColumnMap().get(key) != null) {
                continue;
            }

            Object methodResult = method.invoke(bean);
            Column hBaseColumnValue = new Column(familyName, columnName, methodResult);

            context.getColumnMap().put(key, hBaseColumnValue);
        }

        Class<?> superclass = beanClass.getSuperclass();
        if (superclass == null) {
            return;
        }
        getColumnDataFromBeanMethods(context, bean, superclass);
    }

    // 从列族数据为对象属性赋值
    private static void setBeanFieldsFromFamilyData(Context context, Object bean, Class<?> beanClass) throws IllegalAccessException, InstantiationException {
        if (context == null) {
            throw new IllegalArgumentException("HBase-Helper argument 'context' to fields is null");
        }
        if (beanClass == null) {
            throw new IllegalArgumentException("HBase-Helper argument 'beanClass' to fields is null");
        }

        // 准备工作
        if (context.getRowKey() == null) {
            throw new IllegalStateException("The row-key from Family-Fields-Context is null");
        }
        if (context.getColumnMap() == null) {
            throw new IllegalStateException("The column-map from Family-Fields-Context is null");
        }
        if (bean == null) {
            bean = beanClass.newInstance();
        }

        Field[] fields = beanClass.getDeclaredFields();
        for (Field field : fields) {

            // RowKey 处理
            HBaseRowKey hBaseRowKeyAnnotation = field.getAnnotation(HBaseRowKey.class);
            if (hBaseRowKeyAnnotation != null) {
                HBaseAnnotationScope hBaseRowKeyAnnotationScope = hBaseRowKeyAnnotation.scope();
                if (HBaseAnnotationScope.FIELD != hBaseRowKeyAnnotationScope) {
                    continue;
                }
                if (context.getRowKey() == null) {
                    continue;
                }
                if (context.getRowKey().getValue() == null) {
                    continue;
                }
                Object rowKeyObject = context.getRowKey().getValue();
                setFieldValue(bean, field, rowKeyObject);
                context.getRowKey().setValue(null);
                continue;
            }

            // 获取列族名
            String familyName = getHBaseFamilyNameFromAnnotation(field);
            if (StringUtils.isBlank(familyName)) {
                continue;
            }
            context.setFamilyName(familyName);

            // 获取列名
            HBaseColumn hBaseColumnAnnotation = field.getAnnotation(HBaseColumn.class);
            if (hBaseColumnAnnotation == null) {
                Object fieldObject = field.getType().newInstance();
                setBeanFieldsFromColumnData(context, fieldObject, field.getType());
                setFieldValue(bean, field, fieldObject);
                continue;
            }
            HBaseAnnotationScope hBaseAnnotationScope = hBaseColumnAnnotation.scope();
            if (HBaseAnnotationScope.FIELD != hBaseAnnotationScope) {
                continue;
            }
            String columnName = hBaseColumnAnnotation.name();
            if (StringUtils.isBlank(columnName)) {
                continue;
            }

            // 已经有值时退出本次循环
            String key = String.format(MAP_KEY_FORMAT_HBASE_COLUMN, familyName, columnName);
            Column column = context.getColumnMap().get(key);
            if (column == null) {
                continue;
            }

            setFieldValue(bean, field, column.getColumnValue());
            context.getColumnMap().remove(key);
        }

        if (context.getColumnMap().isEmpty()) {
            return;
        }

        Class<?> superclass = beanClass.getSuperclass();
        if (superclass == null) {
            return;
        }
        setBeanFieldsFromFamilyData(context, bean, superclass);
    }

    // 从对象属性中获得列族数据
    private static void getFamilyDataFromBeanFields(Context context, Object bean, Class<?> beanClass) throws IllegalAccessException {
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
                HBaseAnnotationScope hBaseRowKeyAnnotationScope = hBaseRowKeyAnnotation.scope();
                if (HBaseAnnotationScope.FIELD != hBaseRowKeyAnnotationScope) {
                    continue;
                }
                if (context.getRowKey() == null) {
                    context.setRowKey(new RowKey());
                }
                if (context.getRowKey().getValue() == null) {
                    Object rowKeyObject = getFieldValue(bean, field);
                    context.getRowKey().setValue(rowKeyObject);
                }
                continue;
            }

            // 获取列族名
            String familyName = getHBaseFamilyNameFromAnnotation(field);
            if (StringUtils.isBlank(familyName)) {
                continue;
            }
            context.setFamilyName(familyName);

            // 获取列名
            HBaseColumn hBaseColumnAnnotation = field.getAnnotation(HBaseColumn.class);
            if (hBaseColumnAnnotation == null) {
                Object fieldObject = getFieldValue(bean, field);
                getColumnDataFromBeanFields(context, fieldObject, field.getType());
                continue;
            }
            HBaseAnnotationScope hBaseAnnotationScope = hBaseColumnAnnotation.scope();
            if (HBaseAnnotationScope.FIELD != hBaseAnnotationScope) {
                continue;
            }
            String columnName = hBaseColumnAnnotation.name();
            if (StringUtils.isBlank(columnName)) {
                continue;
            }

            // 已经有值时退出本次循环
            String key = String.format(MAP_KEY_FORMAT_HBASE_COLUMN, familyName, columnName);
            if (context.getColumnMap().get(key) != null) {
                continue;
            }

            Object fieldValue = getFieldValue(bean, field);
            Column hBaseColumnValue = new Column(familyName, columnName, fieldValue);

            context.getColumnMap().put(key, hBaseColumnValue);
        }

        Class<?> superclass = beanClass.getSuperclass();
        if (superclass == null) {
            return;
        }
        getFamilyDataFromBeanFields(context, bean, superclass);
    }

    // 从对象属性中获得列数据
    private static void setBeanFieldsFromColumnData(Context context, Object bean, Class<?> beanClass) throws IllegalAccessException, InstantiationException {
        if (context == null) {
            throw new IllegalArgumentException("The argument 'context' from Family-Fields is null");
        }
        if (beanClass == null) {
            throw new IllegalArgumentException("HBase-Helper argument 'beanClass' to fields is null");
        }

        // 准备工作
        String familyName = context.getFamilyName();
        if (context.getRowKey() == null) {
            throw new IllegalStateException("The row-key from Family-Fields-Context is null");
        }
        if (StringUtils.isBlank(familyName)) {
            throw new IllegalStateException("The family-name from Family-Fields-Context is null");
        }
        if (context.getColumnMap() == null) {
            throw new IllegalStateException("The column-map from Family-Fields-Context is null");
        }
        if (bean == null) {
            bean = beanClass.newInstance();
        }

        Field[] fields = beanClass.getDeclaredFields();
        for (Field field : fields) {

            // 获取列名
            String columnName = getHBaseColumnNameFromAnnotation(field);
            if (StringUtils.isBlank(columnName)) {
                continue;
            }

            // 已经有值时退出本次循环
            String key = String.format(MAP_KEY_FORMAT_HBASE_COLUMN, familyName, columnName);
            Column column = context.getColumnMap().get(key);
            if (column == null) {
                continue;
            }

            setFieldValue(bean, field, column.getColumnValue());
            context.getColumnMap().remove(key);
        }

        if (context.getColumnMap().isEmpty()) {
            return;
        }

        Class<?> superclass = beanClass.getSuperclass();
        if (superclass == null) {
            return;
        }
        setBeanFieldsFromColumnData(context, bean, superclass);
    }

    // 从对象属性中获得列数据
    private static void getColumnDataFromBeanFields(Context context, Object bean, Class<?> beanClass) throws IllegalAccessException {
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
            String columnName = getHBaseColumnNameFromAnnotation(field);
            if (StringUtils.isBlank(columnName)) {
                continue;
            }

            // 已经有值时退出本次循环
            String key = String.format(MAP_KEY_FORMAT_HBASE_COLUMN, familyName, columnName);
            if (context.getColumnMap().get(key) != null) {
                continue;
            }

            Object fieldValue = getFieldValue(bean, field);
            Column hBaseColumnValue = new Column(familyName, columnName, fieldValue);

            context.getColumnMap().put(key, hBaseColumnValue);
        }

        Class<?> superclass = beanClass.getSuperclass();
        if (superclass == null) {
            return;
        }
        getColumnDataFromBeanFields(context, bean, superclass);
    }

    // 从字段属性中获取列族名
    private static String getHBaseFamilyNameFromAnnotation(Field field) {
        HBaseFamily hBaseFamilyAnnotation = field.getAnnotation(HBaseFamily.class);
        if (hBaseFamilyAnnotation == null) {
            return null;
        }
        HBaseAnnotationScope hBaseFamilyAnnotationScope = hBaseFamilyAnnotation.scope();
        if (HBaseAnnotationScope.FIELD != hBaseFamilyAnnotationScope) {
            return null;
        }
        return hBaseFamilyAnnotation.name();
    }

    // 从字段属性中获取列名
    private static String getHBaseColumnNameFromAnnotation(Field field) {
        HBaseColumn hBaseColumnAnnotation = field.getAnnotation(HBaseColumn.class);
        if (hBaseColumnAnnotation == null) {
            return null;
        }
        HBaseAnnotationScope hBaseAnnotationScope = hBaseColumnAnnotation.scope();
        if (HBaseAnnotationScope.FIELD != hBaseAnnotationScope) {
            return null;
        }
        return hBaseColumnAnnotation.name();
    }

    // 获取属性的值
    private static Object getFieldValue(Object bean, Field field) throws IllegalAccessException {
        boolean fieldAccessible = field.isAccessible();
        Object fieldValue = null;
        if (fieldAccessible) {
            fieldValue = field.get(bean);
        } else {
            field.setAccessible(true);
            fieldValue = field.get(bean);
            field.setAccessible(false);
        }
        return fieldValue;
    }

    // 为属性设置值
    private static void setFieldValue(Object bean, Field field, Object value) throws IllegalAccessException {
        boolean fieldAccessible = field.isAccessible();
        if (fieldAccessible) {
            field.set(bean, value);
        } else {
            field.setAccessible(true);
            if (byte.class.equals(field.getType()) || Byte.class.equals(field.getType())) {
                Byte columnValue = Byte.valueOf(value + "");
                field.set(bean, columnValue);
            } else if (short.class.equals(field.getType()) || Short.class.equals(field.getType())) {
                Short columnValue = Short.valueOf(value + "");
                field.set(bean, columnValue);
            } else if (int.class.equals(field.getType()) || Integer.class.equals(field.getType())) {
                Integer columnValue = Integer.valueOf(value + "");
                field.set(bean, columnValue);
            } else if (long.class.equals(field.getType()) || Long.class.equals(field.getType())) {
                Long columnValue = Long.valueOf(value + "");
                field.set(bean, columnValue);
            } else if (float.class.equals(field.getType()) || Float.class.equals(field.getType())) {
                Float columnValue = Float.valueOf(value + "");
                field.set(bean, columnValue);
            } else if (double.class.equals(field.getType()) || Double.class.equals(field.getType())) {
                Double columnValue = Double.valueOf(value + "");
                field.set(bean, columnValue);
            } else if (char.class.equals(field.getType()) || Character.class.equals(field.getType())) {
                Character columnValue = (value + "").charAt(0);
                field.set(bean, columnValue);
            } else if (boolean.class.equals(field.getType()) || Boolean.class.equals(field.getType())) {
                Boolean columnValue = Boolean.valueOf(value + "");
                field.set(bean, columnValue);
            } else {
                field.set(bean, value);
            }
            field.setAccessible(false);
        }
    }



    /* ============================= 以下是私有内部类 ============================= */

    /**
     * 上下文
     */
    protected static class Context {

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
    protected static class RowKey {

        private Object value;
    }

    /**
     * 列对象
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    protected static class Column {

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
