/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.util
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:36
 */
package com.littlehotspot.hadoop.mr.hdfs.util;

/**
 * <h1>常量 - 利用正则表达式清洗 HDFS 文件</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年06月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class CleanByRegexConstant extends MapReduceConstant {

    public static class HadoopConfig {

        public static class Key {

            /**
             * MAPPER 输入正则表达式
             */
            public static final String MAPPER_INPUT_FORMAT_REGEX_PATTERN = Key.class.getName() + ".MAPPER_INPUT_FORMAT_REGEX_PATTERN";

            /**
             * 源数据行数
             */
            public static final String LINE_COUNT_SOURCE_DATA = Key.class.getName() + ".LINE_COUNT_SOURCE_DATA";

            /**
             * 结果行数
             */
            public static final String LINE_COUNT_RESULT_DATA = Key.class.getName() + ".LINE_COUNT_RESULT_DATA";
        }
    }
}
