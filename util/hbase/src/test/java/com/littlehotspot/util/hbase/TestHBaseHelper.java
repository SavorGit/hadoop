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

import com.littlehotspot.util.hbase.model.TestFamilyAttribute;
import com.littlehotspot.util.hbase.model.TestTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.List;

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

    private DecimalFormat decimalFormat;

    private HBaseHelper hBaseHelper;
    private String tableName;
    private String rowKey;

    @Before
    public void init() throws IOException {
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\hadoop-2.7.3");

        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://devpd1:8020");
//        configuration.set("hbase.rootdir", "hdfs://devpd1:8020/hbase");
//        configuration.set("hbase.zookeeper.quorum", "devpd1");
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "localhost");

        hBaseHelper = new HBaseHelper(configuration);
        tableName = "medias";
        rowKey = "FCD5D900B1881496322021";
    }

    @Test
    public void dropTable() {
        hBaseHelper.deleteTable(tableName);
    }

    @Test
    public void createTable() {
        hBaseHelper.createTable(tableName, new String[]{"attr"});
    }

    @Test
    public void insertObject() {
        TestFamilyAttribute attribute = new TestFamilyAttribute("attr00001", 2);
        TestTable table = new TestTable(this.rowKey, "name_001", attribute);
        hBaseHelper.insert(table);
    }

    @Test
    public void getOneRecord() {
        Result result = hBaseHelper.getOneRecord(tableName, this.rowKey);
        hBaseHelper.toBean(result, TestTable.class);
        System.out.println(result);
    }


    @Test
    public void getAllRecord() {
        long start = System.currentTimeMillis();
        List<Result> resultList = hBaseHelper.getAllRecord(tableName);
//        Map<String, List<Result>> result = hBaseHelper.getAllRecord(tableName);
        for (Result result : resultList) {
            System.out.println(result);
        }
        System.out.println(String.format("共查找到 %s 条数据", decimalFormat.format(resultList.size())));
        System.out.println(String.format("用时： %s 毫秒", decimalFormat.format(System.currentTimeMillis() - start)));
    }

    @Test
    public void searchByRowKeyRegex() {
        long start = System.currentTimeMillis();
        List<Result> resultList = hBaseHelper.searchByRowKeyRegex(tableName, "864412032481097\\|.+");
        for (Result result : resultList) {
            System.out.println(result);
        }
        System.out.println(String.format("共查找到 %s 条数据", decimalFormat.format(resultList.size())));
        System.out.println(String.format("用时： %s 毫秒", decimalFormat.format(System.currentTimeMillis() - start)));
    }
}
