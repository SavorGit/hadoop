/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.box.module
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:48
 */
package com.littlehotspot.hadoop.mr.hdfs.module;

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
 * <h1>单元测试 - 利用正则表达式清洗 HDFS 文件</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年06月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestCleanByRegexScheduler {

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
        long start = System.currentTimeMillis();
        try {
            String[] args = {
                    "jobName=Box-Log Clean",
//                    "hdfsCluster=hdfs://devpd1:8020",
//                    "hdfsIn=/home/data/hadoop/flume/box_source/2017-06-28",
//                    "hdfsOut=/home/data/hadoop/flume/box_export/2017-06-28",
                    "hdfsIn=hdfs://devpd1:8020/home/data/hadoop/flume/box/source/command/2017080",
                    "hdfsOut=hdfs://localhost:9000/home/data/hadoop/flume/box/export/2017-08-00",
                    "inMapperRegex=^.+,.*,.*,.*,.*,.*,.*,.*,.*,.*,.*,.*,.+,.*,?$"
            };
            ToolRunner.run(this.conf, new CleanByRegexScheduler(), args);
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
