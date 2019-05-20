/**
 * Copyright (c) 2019, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.module
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:33
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
 *<h1>单元测试 - 利用正则表达式转换 HDFS 文件内容</h1>
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @version 1.0.0.0.1
 * @notes Created on 2019年05月17日<br>
 *        Revision of last commit:$Revision$<br>
 *        Author of last commit:$Author$<br>
 *        Date of last commit:$Date$<br>
 *
 */
public class TestConvertByRegexScheduler extends TestAbstractScheduler {

    @Test
    public void run() {
        long start = System.currentTimeMillis();
        try {
            String[] args = {
                    "jobName=Nginx-Logs Convert",
                    "hdfsIn=file:///d:\\mr-nginx-logs\\source",
                    "hdfsOut=file:///d:\\mr-nginx-logs\\export",
                    "inMapperRegex=^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}) (\\S+) (\\S+) \\[(.*)\\] ([A-Za-z]*) (\\S+) (\\S*) \"(\\d+)\" (\\d+) \"(.*)\" \"(.*)\" \"(.*)\" \"(.*)\"$"
//                    "inMapperRegex=^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}) (\\S+) (\\S+) \\[(.*)\\] ([A-Za-z]*) (\\/content\\/(-?\\d{1,})\\.html\\S*) (\\S*) \"(\\d+)\" (\\d+) \"(.*)\" \"(.*)\" \"(.*)\" \"(.*)\"$"
            };
//            String[] args = {
//                    "jobName=Nginx-Logs Convert",
//                    "hdfsIn=hdfs://onlined1:8020/home/data/hadoop/flume/nginx_logs/admin.redian.com/source/2019-05-15",
//                    "hdfsOut=/home/data/hadoop/flume/nginx_logs/admin.redian.com/export/2019-05-15",
//                    "inMapperRegex=^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}) (\\S+) (\\S+) \\[(.*)\\] ([A-Za-z]*) (\\S+) (\\S*) \"(\\d+)\" (\\d+) \"(.*)\" \"(.*)\" \"(.*)\" \"(.*)\"$"
//            };
            ToolRunner.run(this.conf, new ConvertByRegexScheduler(), args);
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
