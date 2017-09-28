/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:53
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;

import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;

/**
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @version 1.0.0.0.1
 * @notes Created on 2017年09月28日<br>
 *        Revision of last commit:$Revision$<br>
 *        Author of last commit:$Author$<br>
 *        Date of last commit:$Date$<br>
 *
 */
public class BootRateArgument extends Argument{

    public static final Argument StartTime = new Argument("startTime", null, null);// JDBC 驱动
    public static final Argument EndTime = new Argument("endTime", null, null);// JDBC 连接路径
    public static final Argument Issue = new Argument("issue", null, null);// JDBC 连接路径
    public static final Argument ExcelName = new Argument("excelName", null, null);// JDBC 连接路径


    public BootRateArgument(String name, String nullValue, String[] nullArray) {
        super(name, nullValue, nullArray);
    }
}
