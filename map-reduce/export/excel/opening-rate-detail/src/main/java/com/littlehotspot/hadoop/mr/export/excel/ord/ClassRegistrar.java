/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.ord
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:51
 */
package com.littlehotspot.hadoop.mr.export.excel.ord;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <h1>注册器 - 类</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年03月30日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
class ClassRegistrar {

    private static ClassRegistrar DATA_OPERATOR_REGISTRAR = new ClassRegistrar();
    private static ClassRegistrar FIELD_REGISTRAR = new ClassRegistrar();

    private Map<String, Class<?>> classRegistrarMap = new ConcurrentHashMap<>();

    private ClassRegistrar() {
        super();
    }

    static Class<?> dataOperatorRegister(String name, Class<?> type) {
        return DATA_OPERATOR_REGISTRAR.classRegistrarMap.put(name, type);
    }

    public static Class<?> dataOperatorClass(String name) {
        return DATA_OPERATOR_REGISTRAR.classRegistrarMap.get(name);
    }

    static Class<?> fieldRegister(String name, Class<?> type) {
        return FIELD_REGISTRAR.classRegistrarMap.put(name, type);
    }

    public static Class<?> fieldClass(String name) {
        return FIELD_REGISTRAR.classRegistrarMap.get(name);
    }

    // 注册操作器
    static {
        dataOperatorRegister("string", DataOperatorForString.class);
        dataOperatorRegister("date", DataOperatorForDate.class);
    }

    // 注册字段类型
    static {
        fieldRegister("string", String.class);
        fieldRegister("date", Date.class);
    }
}
