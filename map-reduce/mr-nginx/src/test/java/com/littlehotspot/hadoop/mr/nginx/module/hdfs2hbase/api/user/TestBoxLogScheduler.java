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
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.tags.TagsLog;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic.MobileUser;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic.UserDemaLog;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic.UserFinal;
import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SelectModel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.TagList;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.Tags;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.util.List;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestBoxLogScheduler {


    @Test
    public void run() {
        String[] args = {
                "hdfsCluster=hdfs://localhost:9000",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
                "hdfsOut=/home/data/hadoop/flume/text_mr/test-box_log",
                "hbaseRoot=hdfs://localhost:9000/hbase",
                "hbaseZookeeper=localhost"

//                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"
//                "hdfsIn=/home/data/hadoop/flume/test-mr/mob_user",
//                "hdfsOut=/home/data/hadoop/flume/test-mr/test-mob_user",
////                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
//                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\hadoop-2.7.3");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("hbase.master", "devpd1:9000");
//        conf.set("hbase.zookeeper.quorum", "devpd1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://devpd1:9000/hbase");

//		distributedCache
        try {
            ToolRunner.run(conf, new BoxCleanJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void run1() {
        String[] args = {
                "hdfsCluster=hdfs://devpd1:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
                "hdfsOut=/home/data/hadoop/flume/text_mr/test-box_log",
                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1"

//                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"
//                "hdfsIn=/home/data/hadoop/flume/test-mr/mob_user",
//                "hdfsOut=/home/data/hadoop/flume/test-mr/test-mob_user",
////                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
//                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            HBaseHelper hBaseHelper = new HBaseHelper(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
