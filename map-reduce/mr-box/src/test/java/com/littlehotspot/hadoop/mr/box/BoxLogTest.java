/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.box
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 12:50
 */
package com.littlehotspot.hadoop.mr.box;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年06月30日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class BoxLogTest {

    @Test
    public void run() {
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\hadoop-2.7.3");
        String[] args = {"/home/data/hadoop/flume/test-box-source/2017-06-27", "/home/data/hadoop/flume/test-mr/2017-06-27", "^.+,.*,.*,.*,.*,.*,.*,.*,.*,.*,.*,.*,.+,.*,?$"};
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//		distributedCache
        try {
            ToolRunner.run(conf, new BoxLog(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
