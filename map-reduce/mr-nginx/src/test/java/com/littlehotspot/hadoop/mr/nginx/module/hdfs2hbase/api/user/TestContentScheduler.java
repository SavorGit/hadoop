/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:34
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.contentdetail.ConDetail;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.contentdetail.UvCount;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mobilelog.MobileInToHbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestContentScheduler {


    @Test
    public void run() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=/home/data/hadoop/flume/test-mr/mob_user",
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-mobile_log",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase",
                "hdfsIn=/home/data/hadoop/flume/test-mr/mob_user",
                "hdfsOut=/home/data/hadoop/flume/test-mr/test-mob_user",
//                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
//                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("hbase.master", "devpd1:9000");
//        conf.set("hbase.zookeeper.quorum", "devpd1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://devpd1:9000/hbase");

//		distributedCache
        try {
            ToolRunner.run(conf, new ConDetail(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void run2() {
        String[] args = {
                "hdfsIn=/home/data/hadoop/flume/text-mr/test-mobile_log",
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-mobile_logsss",
                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1",
//                "hdfsCluster=hdfs://devpd1:8020",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"


        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new MobileInToHbase(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void run3() {
        String[] args = {
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-content_detail",
                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1",
                "hdfsCluster=hdfs://devpd1:8020",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"


        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new ConDetail(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void run4() {
        String[] args = {
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-content_detail",
                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1",
                "hdfsCluster=hdfs://devpd1:8020",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"


        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new UvCount(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
