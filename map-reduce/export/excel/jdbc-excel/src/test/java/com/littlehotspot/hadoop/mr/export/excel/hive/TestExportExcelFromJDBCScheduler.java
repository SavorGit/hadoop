/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.hive
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 18:28
 */
package com.littlehotspot.hadoop.mr.export.excel.hive;

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
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年06月14日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestExportExcelFromJDBCScheduler {

    private static final String FORMAT_PRINT_TIME_CONSUMING = "执行 %s 用时 %s 毫秒";
    private static final String FORMAT_COLOR_PRINT_TIME_CONSUMING = "@|blue 执行|@ @|green %s|@ @|blue 用时|@ @|red %s|@ @|blue 毫秒|@\n";

    private DecimalFormat decimalFormat;

    private Configuration conf;

    @Before
    public void init() throws IOException {
        System.out.println();
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
//        System.setProperty("oozie.action.conf.xml", "D:\\workflow.xml");
        Locale.setDefault(Locale.CHINA);
        this.decimalFormat = new DecimalFormat();

        this.conf = new Configuration();
//        this.conf.setBoolean(FileOutputFormat.COMPRESS, true);
//        this.conf.set(FileOutputFormat.COMPRESS_CODEC, "org.apache.hadoop.io.compress.GzipCodec");
//        this.conf.setInt(MRJobConfig.NUM_MAPS, 5);
    }

    @Test
    public void run() {
        System.out.println("Test Main Execute ...");
        long start = System.currentTimeMillis();
        try {
            String[] args = {
                    "jobName=Export hive to excel",
                    "jdbcDriver=org.apache.hive.jdbc.HiveDriver",
                    "jdbcUrl=jdbc:hive2://onlinemain:10000/default",
                    "jdbcUsername=",
                    "jdbcPassword=",
                    "workbook=/john.lee/excel/2018年05月01-05月15号开机率明细(hive)",
//                    "workbook=hdfs://onlinemain:8020/john.lee/excel/2018年05月01-05月15号开机率明细(hive).xls",
                    "sheet=媒体表",
                    "title=媒体标识|媒体名称",
                    "jdbcSql=SELECT id, name FROM mysql.savor_media WHERE create_time >= '2017-07-01 00:00:00' AND create_time <= '2017-07-30 23:59:59' ORDER BY id ASC",
//                    "jdbcSql=SELECT id, name, description, creator, create_time, md5, creator_id, oss_addr, file_path, duration, surfix, type, oss_etag, flag, state, checker_id FROM mysql.savor_media WHERE create_time >= '2017-07-01 00:00:00' AND create_time <= '2017-07-30 23:59:59' ORDER BY id ASC",
                    "reduceInRegexValue=^([^\\u0001]+)\\u0001([^\\u0001]*)$"
            };
            ToolRunner.run(this.conf, new ExportExcelFromJDBCScheduler(), args);
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
