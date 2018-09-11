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
                    "jdbcUrl=jdbc:hive2://onlined1:10000/default",
                    "jdbcUsername=",
                    "jdbcPassword=",
                    "workbook=/john.lee/excel/一代单机旧片源(hive)",
                    "sheet=一代旧片源",
                    "title=area_no|hotel_name|room_name|box_mac|media_name",
//                    "jdbcSql=SELECT collect_set(area_no)[0] as area_no,collect_set(hotel_name)[0] as hotel_name,collect_set(room_name)[0] as room_name,box_mac,collect_set(media_name)[0] as media_name FROM archive.box_logs_offline_v1_old_media_20180901 WHERE hotel_name IS NOT NULL GROUP BY box_mac ORDER BY area_no,media_name,hotel_name,room_name,box_mac",
                    "jdbcSql=SELECT collect_set(area_no)[0] AS area_no,collect_set(hotel_name)[0] AS hotel_name,collect_set(room_name)[0] AS room_name,box_mac,collect_set(media_name)[0] AS media_name FROM archive.box_logs_offline_v1_old_media_20180901 WHERE hotel_name IS NOT NULL GROUP BY box_mac ORDER BY area_no,media_name,hotel_name,room_name,box_mac",
                    "reduceInRegexValue=^([^\\u0001]*)\\u0001([^\\u0001]*)\\u0001([^\\u0001]*)\\u0001([^\\u0001]*)\\u0001([^\\u0001]*)$"
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
