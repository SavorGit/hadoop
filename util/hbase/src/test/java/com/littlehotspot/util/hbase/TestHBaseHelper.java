/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.util.hbase
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 09:53
 */
package com.littlehotspot.util.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://devpd1:8020");
        configuration.set("hbase.rootdir", "hdfs://devpd1:8020/hbase");
        configuration.set("hbase.zookeeper.quorum", "devpd1");

        hBaseHelper = new HBaseHelper(configuration);
        tableName = "test_table";
        rowKey = "FCD5D900B1881496322021";
    }

    @Test
    public void dropTable() throws IOException {
        hBaseHelper.deleteTable(tableName);
    }

    @Test
    public void createTable() throws IOException {
        hBaseHelper.createTable(tableName, new String[]{"attr", "ext1","ext2"});
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
