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

import java.text.DecimalFormat;
import java.util.List;
import java.util.Locale;

import static org.fusesource.jansi.Ansi.Color.YELLOW;
import static org.fusesource.jansi.Ansi.ansi;

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

    /**
     * 初始化操作
     */
    @Before
    public void init() {
        Locale.setDefault(Locale.CHINA);
        decimalFormat = new DecimalFormat();

        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\hadoop-2.7.3");

        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS", "hdfs://devpd1:8020");
        configuration.set("hbase.rootdir", "hdfs://devpd1:8020/hbase");
        configuration.set("hbase.zookeeper.quorum", "devpd1");
        hBaseHelper = new HBaseHelper(configuration);
        tableName = "test_hbase";
        rowKey = "rk_00001";

//        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
//        configuration.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
//        configuration.set("hbase.zookeeper.quorum", "localhost");
//        hBaseHelper = new HBaseHelper(configuration);
//        tableName = "medias";
//        rowKey = "FCD5D900B1881496322021";

    }

    /**
     * 删除表
     */
    @Test
    public void dropTable() {
        long start = System.currentTimeMillis();
        hBaseHelper.deleteTable(tableName);
        long end = System.currentTimeMillis();

        String printMessage = String.format("执行 %s 用时 %s 毫秒", Thread.currentThread().getStackTrace()[1].getMethodName(), decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().fg(YELLOW).a(printMessage).reset());

        String ansiPrintMessage = String.format("@|blue 执行|@ @|green %s|@ @|blue 用时|@ @|red %s|@ @|blue 毫秒|@", Thread.currentThread().getStackTrace()[1].getMethodName(), decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().render(ansiPrintMessage));
    }

    /**
     * 创建表
     */
    @Test
    public void createTable() {
        long start = System.currentTimeMillis();
        hBaseHelper.createTable(tableName, new String[]{"basic", "attr"});
        long end = System.currentTimeMillis();

        String printMessage = String.format("执行 %s 用时 %s 毫秒", Thread.currentThread().getStackTrace()[1].getMethodName(), decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().fg(YELLOW).a(printMessage).reset());

        String ansiPrintMessage = String.format("@|blue 执行|@ @|green %s|@ @|blue 用时|@ @|red %s|@ @|blue 毫秒|@", Thread.currentThread().getStackTrace()[1].getMethodName(), decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().render(ansiPrintMessage));
    }

    /**
     * 插入数据
     */
    @Test
    public void insertObject() {
        long start = System.currentTimeMillis();
        TestFamilyAttribute attribute = new TestFamilyAttribute("attr00001", 2);
        TestTable table = new TestTable(this.rowKey, "name_001", attribute);
        hBaseHelper.insert(table);
        long end = System.currentTimeMillis();

        String printMessage = String.format("执行 %s 用时 %s 毫秒", Thread.currentThread().getStackTrace()[1].getMethodName(), decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().fg(YELLOW).a(printMessage).reset());

        String ansiPrintMessage = String.format("@|blue 执行|@ @|green %s|@ @|blue 用时|@ @|red %s|@ @|blue 毫秒|@", Thread.currentThread().getStackTrace()[1].getMethodName(), decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().render(ansiPrintMessage));
    }

    /**
     * 查询一条记录
     */
    @Test
    public void getOneRecord() {
        long start = System.currentTimeMillis();
        Result result = hBaseHelper.getOneRecord(tableName, this.rowKey);
        TestTable testTable = HBaseHelper.toBean(result, TestTable.class);
        System.out.println(testTable);
        long end = System.currentTimeMillis();

        String printMessage = String.format("执行 %s 用时 %s 毫秒", Thread.currentThread().getStackTrace()[1].getMethodName(), decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().fg(YELLOW).a(printMessage).reset());

        String ansiPrintMessage = String.format("@|blue 执行|@ @|green %s|@ @|blue 用时|@ @|red %s|@ @|blue 毫秒|@", Thread.currentThread().getStackTrace()[1].getMethodName(), decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().render(ansiPrintMessage));
    }


    @Test
    public void getAllRecord() {
        long start = System.currentTimeMillis();
        List<Result> resultList = hBaseHelper.getAllRecord(tableName);
//        Map<String, List<Result>> result = hBaseHelper.getAllRecord(tableName);
        for (Result result : resultList) {
            System.out.println(HBaseHelper.toBean(result, TestTable.class));
        }
        System.out.println(String.format("共查找到 %s 条数据", decimalFormat.format(resultList.size())));
        long end = System.currentTimeMillis();

        String printMessage = String.format("执行 %s 用时 %s 毫秒", Thread.currentThread().getStackTrace()[1].getMethodName(), decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().fg(YELLOW).a(printMessage).reset());

        String ansiPrintMessage = String.format("@|blue 执行|@ @|green %s|@ @|blue 用时|@ @|red %s|@ @|blue 毫秒|@", Thread.currentThread().getStackTrace()[1].getMethodName(), decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().render(ansiPrintMessage));
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
