/**
 * Copyright (c) 2019, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.phonebills
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:43
 */
package com.littlehotspot.hadoop.mr.phonebills;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.fusesource.jansi.Ansi;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.fusesource.jansi.Ansi.ansi;

/**
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @version 1.0.0.0.1
 * @notes Created on 2019年04月02日<br>
 *        Revision of last commit:$Revision$<br>
 *        Author of last commit:$Author$<br>
 *        Date of last commit:$Date$<br>
 *
 */
public class TestPhoneBillsScheduler {

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
                    "hdfsIn=hdfs://onlinemain:8020/home/data/phonebills/source/",
                    "hdfsOut=hdfs://onlinemain:8020/home/data/phonebills/export/lizhao/"
            };
            ToolRunner.run(this.conf, new PhoneBillsScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();

        String printMessage = String.format(FORMAT_PRINT_TIME_CONSUMING, Thread.currentThread().getStackTrace()[1].getMethodName(), this.decimalFormat.format(end - start));
        System.out.println(Ansi.ansi().eraseScreen().fg(Ansi.Color.YELLOW).a(printMessage).reset());

        String ansiPrintMessage = String.format(FORMAT_COLOR_PRINT_TIME_CONSUMING, Thread.currentThread().getStackTrace()[1].getMethodName(), this.decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().render(ansiPrintMessage));
    }

    @Test
    public void aaa(){
        String data="20190103145309343232AC102A1489159900|900002|0|075523997630|02022512715|1|02022512715|075523997630|20190103145255|20190103145309|14|102||0||172.16.46.22|C20190103145255AC102f20eb5fb28d|075523997630|02022512715|||99|3214|1|1||1|14||f20eb5fb-28d1-4c31-bdb7-85a195a83442|20190103145250|media/20190103/3214/2019010314/YZMIFS03_f20eb5fb-28d1-4c31-bdb7-85a195a83442_075523997630_+862022512715.wav|172.16.46.22|20190103145250|99|";
//        String data="20190103145357031413AC102A8902290533|900002|0|02868859999|02863205941|1|02863205941|02868859999|20190103145356|20190103145357|1|102||0||172.16.46.20|C20190103145356AC102656a9229ab7|02868859999|02863205941|||200|1006|1|0||1|1||656a9229-ab7b-4946-9a56-b367c6f876ce|20190103145350|media/20190103/1006/2019010314/YZMIFS01_656a9229-ab7b-4946-9a56-b367c6f876ce_+862868859999_+862863205941.wav|172.16.46.20|20190103145350|99|sip:200|";
//        String data="20190103145355313513AC102A3381842867|900002|0|07506700718|01086390624|1|01086390624|07506700718|20190103145141|20190103145355|134|102||0||172.16.46.23|C20190103145141AC10278ab792e757|07506700718|01086390624|||200|3003|1|1||1|134||78ab792e-7574-440f-99df-3dfef8b3cad6|20190103145122|media/20190103/3003/2019010314/YZMIFS04_78ab792e-7574-440f-99df-3dfef8b3cad6_07506700718_86390624.wav|172.16.46.23|20190103145122|99|sip:200|";
        Pattern _splitAreaPhonePattern = Pattern.compile("^([^|]*\\|[^|]*\\|[^|]*\\|0(?:(?:[12][0-9])|(?:[3456789][0-9]{2})))([^|]+\\|.+)$");
        Matcher _splitAreaPhoneMatcher = _splitAreaPhonePattern.matcher(data);
        if (_splitAreaPhoneMatcher.find()) {
            System.out.println(_splitAreaPhoneMatcher.group(1) + "        " + _splitAreaPhoneMatcher.group(2));
        } else {
            System.out.println();
        }
    }
}
