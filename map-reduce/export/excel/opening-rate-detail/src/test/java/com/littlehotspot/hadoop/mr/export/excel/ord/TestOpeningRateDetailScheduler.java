/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.ord
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:33
 */
package com.littlehotspot.hadoop.mr.export.excel.ord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.fusesource.jansi.Ansi;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Locale;

import static org.fusesource.jansi.Ansi.ansi;

/**
 * <h1>测试类</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年03月30日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestOpeningRateDetailScheduler {

    private static final String FORMAT_PRINT_TIME_CONSUMING = "执行 %s 用时 %s 毫秒";
    private static final String FORMAT_COLOR_PRINT_TIME_CONSUMING = "@|blue 执行|@ @|green %s|@ @|blue 用时|@ @|red %s|@ @|blue 毫秒|@\n";

    private DecimalFormat decimalFormat;

    private Configuration conf;

    @Before
    public void init() throws IOException {
        System.out.println();
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        Locale.setDefault(Locale.CHINA);
        this.decimalFormat = new DecimalFormat();

        this.conf = new Configuration();

//        conf.set("fs.defaultFS", "hdfs://onlinemain:8020");
//        this.conf.setBoolean("dfs.permissions", false);
//        this.conf.set("mapred.job.tracker", "local");
//        this.conf.set("mapreduce.framework.name", "yarn");
//        this.conf.set("yarn.resoucemanager.hostname", "localhost");
//        this.conf.set("yarn.resourcemanager.scheduler.address", "localhost:8030");
//        this.conf.set("yarn.resourcemanager.address", "localhost:8032");
//        this.conf.set("yarn.resourcemanager.resource-tracker.address", "localhost:8035");
//        this.conf.set("yarn.resourcemanager.admin.address", "localhost:8033");
    }

    @Test
    public void run() {
        System.out.println();
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        System.setProperty("oozie.action.conf.xml", "D:\\workflow.xml");
        long start = System.currentTimeMillis();
        try {
            String[] args = {
                    "inMapperRegex=^[^\\u0001]+\\u0001[^\\u0001]+\\u0001[^\\u0001]+\\u0001\\d+\\u0001[^\\u0001]+\\u0001\\d+\\u0001(\\d{8})\\u0001[0-9.]+\\u0001[0-9.]+\\u0001[0-9.]+\\u0001[A-Za-z0-9]{12}$",
//                  "inMapperRegex=^[^\\u0001]+\\u0001[^\\u0001]+\\u0001[^\\u0001]+\\u0001\\d+\\u0001[^\\u0001]+\\u0001\\d+\\u0001(\\d{8})\\u0001[0-9.]+\\u0001[0-9.]+\\u0001[0-9.]+\\u0001[A-Fa-f0-9]{12}$",
                    "workbook=hdfs://onlinemain:8020/john.lee/excel/2018年03月01-03月15号开机率明细(hive).xls",

                    // Sheet 0
                    "hdfsIn=hdfs://onlinemain:8020/home/data/hadoop/hive/box_log_play_rate",
                    "sheet=0,box_log_play_rate,网络版,\\u0001",// sheetIndex,hdfs目录名,sheet名称,数据中字段的分割符
                    "title=0,\\|,区域|酒楼|位置|包间|维护人|重点酒楼|播放日期|机顶盒编号|播放次数|播放总秒数|开机率",// sheetIndex,标题分割符,字段名1|字段名2|...
                    "operationMode=0,1,>=,20180301,yyyyMMdd,date,date",// sheet索引,正则GROUP索引,操作符号,参考值,字段格式,字段类型
                    "operationMode=0,1,<=,20180331,yyyyMMdd,date,date",

                    // Sheet 1
                    "hdfsIn=hdfs://onlinemain:8020/home/data/hadoop/hive/stand_alone_play_rate",
                    "sheet=1,stand_alone_play_rate,一代单机版,\\u0001",
                    "title=1,\\|,区域|酒楼|位置|包间|维护人|重点酒楼|播放日期|机顶盒编号|播放次数|播放总秒数|开机率",
                    "operationMode=1,1,>=,20180301,yyyyMMdd,date,date",
                    "operationMode=1,1,<=,20180331,yyyyMMdd,date,date",

                    // Sheet 2
                    "hdfsIn=hdfs://onlinemain:8020/home/data/hadoop/hive/stand3_box_log_play_rate",
                    "sheet=2,stand3_box_log_play_rate,三代单机版,\\u0001",
                    "title=2,\\|,区域|酒楼|位置|包间|维护人|重点酒楼|播放日期|机顶盒编号|播放次数|播放总秒数|开机率",
                    "operationMode=2,1,>=,20180301,yyyyMMdd,date,date",
                    "operationMode=2,1,<=,20180331,yyyyMMdd,date,date"
            };
            ToolRunner.run(this.conf, new OpeningRateDetailScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();

        String printMessage = String.format(FORMAT_PRINT_TIME_CONSUMING, Thread.currentThread().getStackTrace()[1].getMethodName(), this.decimalFormat.format(end - start));
        System.out.println(Ansi.ansi().eraseScreen().fg(Ansi.Color.YELLOW).a(printMessage).reset());

        String ansiPrintMessage = String.format(FORMAT_COLOR_PRINT_TIME_CONSUMING, Thread.currentThread().getStackTrace()[1].getMethodName(), this.decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().render(ansiPrintMessage));
    }
}
