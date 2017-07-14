/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 09:53
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年06月20日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestHBaseHelper {

    private HBaseHelper hBaseHelper;
    private String tableName;
    private String rowKey;

    @Before
    public void init() throws IOException {
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\hadoop-2.7.3");
        hBaseHelper = new HBaseHelper();
        tableName = "user";
        rowKey = "a000005585fe18";
    }

    @Test
    public void dropTable() throws IOException {
        hBaseHelper.deleteTable(tableName);
    }

    @Test
    public void createTable() throws IOException {
        hBaseHelper.createTable(tableName, new String[]{"basic", "info"});
    }

    @Test
    public void getOneRecord() throws IOException {
        Result result = hBaseHelper.getOneRecord(tableName, rowKey);
        System.out.println(result);
    }


    @Test
    public void getAllRecord() throws IOException {
        long start = System.currentTimeMillis();
//        List<Result> result = hBaseHelper.getAllRecord(tableName);
//        Map<String, List<Result>> result = hBaseHelper.getAllRecord(tableName);
//        System.out.println(result.size());
//        System.out.println(System.currentTimeMillis() - start);
    }
}
