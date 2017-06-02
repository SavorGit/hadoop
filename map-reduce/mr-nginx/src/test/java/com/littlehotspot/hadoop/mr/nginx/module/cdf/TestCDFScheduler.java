/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.cdf
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 14:32
 */
package com.littlehotspot.hadoop.mr.nginx.module.cdf;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * <h1>测试 - 数据格式转换</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestCDFScheduler {

    @Test
    public void run() {
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\hadoop-2.7.3");
        String[] args = {
                "hdfsCluster=hdfs://onlinemain:8020",
                "hdfsIn=file:///F:\\工作环境软件\\Hadoop\\nginx_log",
                "inRegex=^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}) - [^ ]+ \\[(.+)\\] ([A-Z]+) ([^ ]+) HTTP/[^ ]+ \"(\\d{3})\" \\d+ \"(.+)\" \"(.*deviceid.*)\" \"(.+)\" \"(.+)\"$",
                "hdfsOut=/home/data/hadoop/flume/test-mr/2017-05-15"
        };
        Configuration conf = new Configuration();

//		distributedCache
        try {
            ToolRunner.run(conf, new CDFScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void date() {
        try {
            String dateString = "28/Mar/2017:11:13:23 +0800";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
            Date date = simpleDateFormat.parse(dateString);
            System.out.println(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void date_2() {
        String pattern = "dd/MMM/yyyy:HH:mm:ss Z";
        Date date = new Date();
        System.out.println(DateFormatUtils.format(date, pattern, Locale.US));
    }
}
