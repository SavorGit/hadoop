/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.mobile.hdfs2hbase.hfile
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 16:22
 */
package com.littlehotspot.hadoop.mr.nginx.mobile.hdfs2hbase.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * <h1>测试 - 通过 HFile 方式把 HDFS 数据导入到 HBase</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月25日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class HFileSchedulerTest {

    @Test
    public void run() {
        String[] args = {"hdfsIn=hdfs://devpd1:8020/home/data/hadoop/flume/test-mr/2017-05-22", "hdfsOut=hdfs://devpd1:8020/home/data/hadoop/flume/test-mr/hfile/blog", "table=blog"};

        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\hadoop-2.7.3");

        Configuration conf = HBaseConfiguration.create();
//        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("hbase.master", "devpd1:9000");
//        conf.set("hbase.zookeeper.quorum", "devpd1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://devpd1:9000/hbase");

//		distributedCache
        try {
            ToolRunner.run(conf, new HFileScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
