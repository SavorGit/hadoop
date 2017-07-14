/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.util.argument.model
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 13:59
 */
package com.littlehotspot.util.argument.model;

import net.lizhaoweb.common.util.argument.model.AbstractArgument;

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
public class Argument extends AbstractArgument {

    public static final Argument JobName = new Argument("jobName", null, null);// 任务名称
    public static final Argument InputPath = new Argument("hdfsIn", null, null);// 输入的 HDFS 路径
    public static final Argument OutputPath = new Argument("hdfsOut", null, null);// 输出的 HDFS 路径

    public static final Argument HDFSCluster = new Argument("hdfsCluster", null, null);// HDFS 集群地址
    public static final Argument MapperInputFormatRegex = new Argument("inMapperRegex", null, null);// 输入 Mapper 时的正则匹配
    public static final Argument ReduceInputFormatRegex = new Argument("inReduceRegex", null, null);// 输入 Reduce 时的正则匹配

    public static final Argument HBaseCluster = new Argument("hBaseCluster", null, null);// HBase 集群地址
    public static final Argument HbaseTable = new Argument("hBaseTable", null, null);// HBASE 表名

    public static final Argument HiveCluster = new Argument("hiveCluster", null, null);// Hive 集群地址

    public Argument(String name, String nullValue, String[] nullArray) {
        super(name, nullValue, nullArray);
    }
}
