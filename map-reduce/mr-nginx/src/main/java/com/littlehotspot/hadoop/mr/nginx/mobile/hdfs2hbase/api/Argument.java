/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.mobile.hdfs2hbase.api
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:48
 */
package com.littlehotspot.hadoop.mr.nginx.mobile.hdfs2hbase.api;

import lombok.Getter;

/**
 * <h1>枚举 - 参数</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月24日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public enum Argument {
    HDFSCluster("hdfsCluster", null),
    MapperInputFormatRegex("inRegex", null),
    HbaseTable("table", null),
    InputPath("hdfsIn", null),
    OutputPath("hdfsOut", null);

    @Getter
    private String name;

    @Getter
    private String defaultValue;

    private Argument(String name, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }
}
