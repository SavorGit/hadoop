package com.littlehotspot.hadoop.mr.box; /**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : PACKAGE_NAME
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 16:00
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月16日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class MobileLogTest {

    @Test
    public void run() {
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\hadoop-2.7.3");
        String[] args = {"/home/data/hadoop/flume/mobile_source/2017-05-15", "/home/data/hadoop/flume/test-mr-mobile/2017-05-15"};
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//		distributedCache
        try {
            ToolRunner.run(conf, new com.littlehotspot.hadoop.mr.mobile.MobileLog(), args);
        } catch (Exception e) {
            // TODO: handle exception
            e.toString();
        }
    }
}
