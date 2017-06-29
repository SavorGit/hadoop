/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.util
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 13:59
 */
package com.littlehotspot.hadoop.mr.hdfs.util;

import lombok.Getter;

/**
 * <h1>枚举 - 参数</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public enum Argument {
    HDFSCluster("hdfsCluster", null),// HDFS 集群地址
    MapperInputFormatRegex("inRegex", null),// 输入时的正则匹配
    InputPath("hdfsIn", null),// 输入的 HDFS 路径
    OutputPath("hdfsOut", null),// 输出的 HDFS 路径
    HbaseTable("table", null),// HBASE 表名
    ;

    /**
     * 参数名
     */
    @Getter
    private String name;

    /**
     * 参数默认值
     */
    @Getter
    private String defaultValue;

    private Argument(String name, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }
}
