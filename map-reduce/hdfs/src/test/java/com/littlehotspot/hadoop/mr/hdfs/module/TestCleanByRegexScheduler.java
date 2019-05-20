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

import org.apache.hadoop.util.ToolRunner;
import org.fusesource.jansi.Ansi;
import org.junit.Test;

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
public class TestCleanByRegexScheduler extends TestAbstractScheduler {

    @Test
    public void run() {
        long start = System.currentTimeMillis();
        try {
            String[] args = {
                    "jobName=Box-Log Clean",
//                    "hdfsCluster=hdfs://devpd1:8020",
//                    "hdfsIn=/home/data/hadoop/flume/box_source/2017-06-28",
//                    "hdfsOut=/home/data/hadoop/flume/box_export/2017-06-28",
                    "hdfsIn=hdfs://devpmain:8020/home/data/hadoop/flume/box_source/2017-12-01",
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
