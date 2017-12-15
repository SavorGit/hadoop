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

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate.BoxCleanJob;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.heartbeat.FromMysql;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediabox.InToHbase;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediabox.InToMysql;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediabox.StaOfCaa;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediacheck.MacMedia;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediacheck.MediaMac;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediacheck.MediaMacToHbase;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mobilelog.MobileInToHbase;
import org.apache.commons.lang.StringUtils;
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
public class TestMediaScheduler {


    @Test
    public void run() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-box_log",
                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase",
                "hbaseZookeeper=devpd1",
                "time=2017080100"

        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new MediaMac(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void run2() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-box_log2",
                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase",
                "hbaseZookeeper=devpd1",
                "time=2017080100"


        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new MacMedia(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void run3() {
        String[] args = {
                "hdfsIn=/home/data/hadoop/flume/text-mr/test-medias",
                "hdfsOut=/home/data/hadoop/flume/text_mr/test-hbase/medias",
                "hbaseRoot=hdfs://onlinemain:8020/hbase",
                "hbaseZookeeper=onlinemain",
                "hdfsCluster=hdfs://onlinemain:8020",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170512162404/hbase",
                "tableName=mac_media"

        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new MediaMacToHbase(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
