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
package com.littlehotspot.hadoop.mr.box.module;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * <h1>单元测试 - 机顶盒日志第一次清洗</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年06月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestLogClean1Scheduler {

    @Test
    public void run() {
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\hadoop-2.7.3");
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/box_source/2017-06-27",
                "hdfsOut=/home/data/hadoop/flume/test-mr/2017-06-27"
        };
        try {
            ToolRunner.run(null, new LogClean1Scheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
