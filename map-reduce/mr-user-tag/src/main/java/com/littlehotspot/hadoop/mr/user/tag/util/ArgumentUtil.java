/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.util
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:32
 */
package com.littlehotspot.hadoop.mr.user.tag.util;

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
    private static Map<String, List<String>> parameters;

    public static void init(String[] args) {
        if (args == null) {
            throw new IllegalArgumentException("Argument 'args' is null");
        }
        parameters = analysisArgument(args);
    }

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
     * @param parameterName 参数名
     * @return List
     */
    public static String getParameterValues(String parameterName) {
        if (parameters == null) {
            throw new IllegalArgumentException("Argument 'parameters' is null");
        }
        if (parameterName == null) {
            throw new IllegalArgumentException("Argument 'parameterName' is null");
        }
        return parameters.get(parameterName).get(0);
    }

}
