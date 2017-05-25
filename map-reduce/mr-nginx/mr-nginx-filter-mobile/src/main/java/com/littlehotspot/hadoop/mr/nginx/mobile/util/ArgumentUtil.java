/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.mobile.h2h.util
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:32
 */
package com.littlehotspot.hadoop.mr.nginx.mobile.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1>工具类 - 参数</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月24日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class ArgumentUtil {

    /**
     * 解析参数。
     * <p>
     * 将参数数组解析成参数 MAP
     * 如：
     * ["a1=con1","b=con2","a1=con3"]  ====>  {"a1"=["con1","con3"],"b"=["con2"]}
     *
     * @param args 参数数组
     * @return Map
     */
    public static Map<String, List<String>> analysisArgument(String[] args) {
        if (args == null) {
            throw new IllegalArgumentException("Argument 'args' is null");
        }
        if (args.length < 1) {
            return new ConcurrentHashMap<>();
        }
        Map<String, List<String>> parameterMap = new ConcurrentHashMap<>();
        Pattern pattern = Pattern.compile("^([^=]+)=(.*)$");
        for (String arg : args) {
            Matcher matcher = pattern.matcher(arg);
            if (!matcher.find()) {
                continue;
            }
            String key = matcher.group(1);
            String value = matcher.group(2);
            List<String> valueList = parameterMap.get(key);
            if (valueList == null) {
                valueList = new ArrayList<>();
            }
            valueList.add(value);
            parameterMap.put(key, valueList);
        }
        return parameterMap;
    }

    /**
     * 获取参数的值。
     *
     * @param parameters    参数 Map
     * @param parameterName 参数名
     * @return List
     */
    public static List<String> getParameterValues(Map<String, List<String>> parameters, String parameterName) {
        if (parameters == null) {
            throw new IllegalArgumentException("Argument 'parameters' is null");
        }
        if (parameterName == null) {
            throw new IllegalArgumentException("Argument 'parameterName' is null");
        }
        return parameters.get(parameterName);
    }

    /**
     * 获取参数的值。
     *
     * @param parameters    参数 Map
     * @param parameterName 参数名
     * @param defaultValue  默认值。
     *                      1、参数 Map 中的值(List) 为 null 或大小为 0 时，此时的返回值。
     *                      2、参数 Map 中的值(List) 的第一个元素为 null ，此时的返回值。
     * @return String
     */
    public static String getParameterValue(Map<String, List<String>> parameters, String parameterName, String defaultValue) {
        List<String> valueList = getParameterValues(parameters, parameterName);
        if (valueList == null) {
            return defaultValue;
        }
        if (valueList.size() < 1) {
            return defaultValue;
        }
        String value = valueList.get(0);
        return value == null ? defaultValue : value;
    }

    /**
     * 获取参数的值。
     *
     * @param parameters    参数 Map
     * @param parameterName 参数名
     * @return String
     */
    public static String getParameterValue(Map<String, List<String>> parameters, String parameterName) {
        return getParameterValue(parameters, parameterName, null);
    }
}
