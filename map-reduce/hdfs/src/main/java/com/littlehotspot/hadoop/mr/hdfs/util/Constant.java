/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.util
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 11:05
 */
package com.littlehotspot.hadoop.mr.hdfs.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Map;

/**
 * <h1>常量 - 通用</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月24日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class Constant {
    public static final String DATA_FORMAT_1 = "dd/MMM/yyyy:HH:mm:ss Z";
    public static final String DATA_FORMAT_2 = "yyyy-MM-dd HH:mm:ss Z";
    public static final char HIVE_CONTENT_SPLIT_CHAR = 0x0001;// HIVE 文件内容默认分割符

    public static class CommonVariables {

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
            String value = ArgumentUtil.getParameterValue(parameters, argument.getName(), argument.getDefaultValue());
            return value;
        }

        /**
         * 解析参数并初始化 MapReduce
         *
         * @param configuration MapReduce 配置对象
         * @param args          参数列表
         */
        public static void initMapReduce(Configuration configuration, String[] args) {
            analysisArgument(args);

            // 配置 HDFS 根路径
            String hdfsCluster = getParameterValue(Argument.HDFSCluster);
            if (StringUtils.isNotBlank(hdfsCluster)) {
                configuration.set("fs.defaultFS", hdfsCluster);
            }
        }
    }
}
