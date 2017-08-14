/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 13:06
 */
package com.littlehotspot.hadoop.mr.nginx;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.CommonVariables;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年07月14日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestRegex {

    @Test
    public void testNginxLog() {
        String data = "100.109.253.0|1497994915000|POST|/version/Upgrade/index?time=1497994915&sign=778ae1a9076b408742464d1ebe841db7&deviceId=869555020891456&deviceToken=Ao2rDnwwiYmsQ0XQyI6lDTe4FtTmqkzwCCg5xhNzZP5u|200||2.3|2017061201|5.0.2|21|vivo Y31|hotSpot|android|10001|xiaomi|869555020891456|1||okhttp/3.3.0|101.38.37.100|2017-06-21 05:41:55 +0800\t";

        long start = System.currentTimeMillis();
        Matcher matcher = CommonVariables.MAPPER_NGINX_LOG_FORMAT_REGEX.matcher(data);
        if (!matcher.find()) {
            System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
        } else {
            System.out.println(System.currentTimeMillis() - start + "\t" + matcher.groupCount() + "\t" + matcher.group(21));
        }
    }

    @Test
    public void testTime() {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(sdf.parse("2017-08-01"));
            calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH) - 1, 0, 0, 0);
            System.out.println(calendar.getTime() + "\t" + calendar.getTime().getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
