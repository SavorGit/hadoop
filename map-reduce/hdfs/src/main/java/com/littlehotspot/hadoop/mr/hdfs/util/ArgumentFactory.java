/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.util
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 13:18
 */
package com.littlehotspot.hadoop.mr.hdfs.util;

import java.util.List;
import java.util.Map;

/**
 * <h1>工厂类 - 参数</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年06月30日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class ArgumentFactory {

    // 参数
    private static Map<String, List<String>> parameters;

    /**
     * 解析参数
     *
     * @param args 参数列表
     */
    public static void analysisArgument(String[] args) {
        if (args == null) {
            throw new IllegalArgumentException("Argument 'args' is null");
        }
        parameters = ArgumentUtil.analysisArgument(args);
    }

    /**
     * 获取参数值
     *
     * @param argument 参数
     * @return 参数值
     */
    public static String getParameterValue(Argument argument) {
        if (argument == null) {
            throw new IllegalArgumentException("Argument 'argument' is null");
        }
        return ArgumentUtil.getParameterValue(parameters, argument.getName(), argument.getDefaultValue());
    }
}
